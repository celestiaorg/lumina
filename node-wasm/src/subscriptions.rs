use std::cell::RefCell;
use std::rc::Rc;

use futures::{Stream, StreamExt};
use js_sys::{AsyncIterator, Promise, Reflect, Symbol};
use lumina_node::node::subscriptions::SubscriptionError;
use lumina_utils::executor::spawn;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_wasm_bindgen::{from_value, to_value};
use tokio::sync::Mutex;
use tracing::{debug, error, warn};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::future_to_promise;
use web_sys::{MessageChannel, MessagePort};

use crate::error::{Context, Error, Result};
use crate::ports::{MessagePortLike, split_port};
use crate::utils::MessageChannelExt;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
pub(crate) struct SubscriptionReceiverReady;

/// Error thrown while processing subscription
#[wasm_bindgen(getter_with_clone, js_name = "SubscriptionError")]
#[derive(Debug, Serialize, Deserialize)]
pub struct JsSubscriptionError {
    /// Height at which the error occurred, if applicable
    pub height: Option<u64>,
    /// error message
    pub error: String,
}

#[wasm_bindgen(getter_with_clone)]
pub struct IteratorResultObject {
    /// Has the value true if the iterator is past the end of the
    /// iterated sequence. In this case value optionally specifies
    /// the return value of the iterator.
    pub done: bool,
    /// Any JavaScript value returned by the iterator.
    /// Can be omitted when done is true.
    pub value: JsValue,
}

impl IteratorResultObject {
    pub fn done() -> Self {
        Self {
            done: true,
            value: JsValue::UNDEFINED,
        }
    }

    pub fn ready(value: JsValue) -> Self {
        Self { done: false, value }
    }
}

#[wasm_bindgen(skip_typescript)]
#[derive(Clone)]
pub struct AsyncIteratorImpl {
    next: Rc<Closure<dyn FnMut() -> Promise>>,
}

impl AsyncIteratorImpl {
    fn prepare_async_iterator_symbol(iterator: &JsValue) -> Result<()> {
        let return_self_method = Reflect::get(iterator, &"return_self".into())?;
        Reflect::set(iterator, &Symbol::async_iterator(), &return_self_method)?;
        Ok(())
    }

    pub fn into_raw(self) -> JsValue {
        self.into()
    }
}

#[wasm_bindgen]
impl AsyncIteratorImpl {
    pub fn next(&self) -> Promise {
        Reflect::apply(
            JsCast::unchecked_ref::<js_sys::Function>(self.next.as_ref().as_ref()),
            &JsValue::UNDEFINED,
            &js_sys::Array::new(),
        )
        .expect("apply on closure should succeed")
        .unchecked_into()
    }

    pub fn return_self(&self) -> JsValue {
        JsValue::from(self.clone())
    }
}

impl From<Closure<dyn FnMut() -> Promise>> for AsyncIteratorImpl {
    fn from(next: Closure<dyn FnMut() -> Promise>) -> Self {
        AsyncIteratorImpl {
            next: Rc::new(next),
        }
    }
}

// Wrap provided port into SubscriptionStream and prepare it to be used as
// js AsyncIterator. Assumes provided port was prepared with [`forward_stream_to_message_port`]
pub(crate) fn into_async_iterator<S>(port: MessagePortLike) -> Result<AsyncIterator>
where
    S: DeserializeOwned + Into<JsValue> + 'static,
{
    let (feedback, receiver) = split_port(port)?;

    let feedback = Rc::new(RefCell::new(feedback));
    let receiver = Rc::new(Mutex::new(
        receiver
            .map(|result| {
                from_value::<Result<S, JsSubscriptionError>>(result.data()).map_err(|e| {
                    JsSubscriptionError {
                        height: None,
                        error: format!("error deserializing subscription item: {e}"),
                    }
                })?
            })
            .boxed_local(),
    ));

    let async_iterator: AsyncIteratorImpl = Closure::<dyn FnMut() -> Promise>::new(move || {
        let (receiver, feedback) = (receiver.clone(), feedback.clone());
        future_to_promise(async move {
            if let Err(e) = feedback.borrow().send(&SubscriptionReceiverReady, &[]) {
                return Err(to_value(&e).unwrap());
            }

            let Some(next) = receiver.lock().await.next().await else {
                return Ok(IteratorResultObject::done().into());
            };

            Ok(match next {
                Ok(item) => IteratorResultObject::ready(item.into()),
                // Forward error and let user catch it
                Err(error) => IteratorResultObject::ready(error.into()),
            }
            .into())
        })
    })
    .into();
    let value = async_iterator.into_raw();
    AsyncIteratorImpl::prepare_async_iterator_symbol(&value)?;

    Ok(value.unchecked_into())
}

// Spawn a task responsible for sending items from the provided stream as they become available
// and as the receiving end signals its readiness with SubscriptionReceiverReady.
pub(crate) fn forward_stream_to_message_port<T>(
    mut stream: impl Stream<Item = Result<T, SubscriptionError>> + Unpin + 'static,
) -> Result<MessagePort>
where
    T: Serialize + Unpin + 'static,
{
    let (p0, p1) = MessageChannel::new_ports()?;

    let (subscription_sender, event_receiver) = split_port(p0.into())?;
    let mut feedback_receiver = event_receiver.map(|ev| {
        from_value::<SubscriptionReceiverReady>(ev.data())
            .context("could not deserialize subscription signal")
    });

    spawn(async move {
        loop {
            let Some(feedback) = feedback_receiver.next().await else {
                break;
            };
            let _ = feedback.inspect_err(|e| warn!("Error receiving subscription feedback: {e}"));

            let item: Result<Option<T>> = stream.next().await.transpose().map_err(Error::from);
            let _ = subscription_sender.send(&item, &[]).inspect_err(|e| {
                error!("Error sending subscription item: {e}");
            });
        }
        debug!("Ending subscription");
    });

    Ok(p1)
}

#[cfg(test)]
mod tests {
    use crate::utils::MessageChannelExt;

    use super::*;

    use futures::StreamExt;
    use js_sys::Array;
    use lumina_utils::executor::spawn;
    use serde_wasm_bindgen::from_value;
    use wasm_bindgen_test::*;
    use web_sys::MessageChannel;

    #[wasm_bindgen(module = "/test/async_iterator.js")]
    extern "C" {
        async fn drain_async_iterator(iterator: JsValue) -> Array;
    }

    #[wasm_bindgen_test]
    async fn smoke() {
        let (p0, p1) = MessageChannel::new_ports().unwrap();

        let (tx, rx) = split_port(p0.into()).unwrap();
        let mut rx = rx.map(|ev| from_value(ev.data()).unwrap());
        let iterator = into_async_iterator::<String>(p1.into()).unwrap();

        spawn(async move {
            let msg: Result<String, JsSubscriptionError> = Ok("hello".to_string());
            tx.send(&msg, &[]).unwrap();
            let feedback = rx.next().await;
            assert_eq!(feedback, Some(SubscriptionReceiverReady));

            let msg: Result<String, JsSubscriptionError> = Ok("world".to_string());
            tx.send(&msg, &[]).unwrap();
            let feedback = rx.next().await;
            assert_eq!(feedback, Some(SubscriptionReceiverReady));

            drop(tx)
        });

        let received: Vec<_> = drain_async_iterator(iterator.into())
            .await
            .iter()
            .map(|v| from_value::<String>(v).unwrap())
            .collect();
        assert_eq!(received.as_ref(), ["hello", "world"]);
    }

    #[wasm_bindgen_test]
    async fn close() {
        let (p0, p1) = MessageChannel::new_ports().unwrap();

        let (tx, rx) = split_port(p0.into()).unwrap();
        let async_iterator = into_async_iterator::<String>(p1.into());
        drop(async_iterator);

        tx.send(&"foo".to_string(), &[]).unwrap();

        let mut rx = rx.map(|ev| from_value::<SubscriptionReceiverReady>(ev.data()).unwrap());
        assert!(rx.next().await.is_none());
        assert!(rx.next().await.is_none());
    }
}
