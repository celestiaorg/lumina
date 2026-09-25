use core::marker::PhantomData;

use prost::Message;
use tonic::Status;
use tonic::codec::{Codec, EncodeBuf, Encoder, ProstCodec};

#[derive(Debug, Clone)]
pub struct PreallocProstCodec<T, U> {
    marker: PhantomData<(T, U)>,
}

impl<T, U> Default for PreallocProstCodec<T, U> {
    fn default() -> Self {
        Self {
            marker: PhantomData,
        }
    }
}

impl<T, U> Codec for PreallocProstCodec<T, U>
where
    T: Message + Send + 'static,
    U: Message + Default + Send + 'static,
{
    type Encode = T;
    type Decode = U;
    type Encoder = PreallocProstEncoder<T>;
    type Decoder = <ProstCodec<T, U> as Codec>::Decoder;

    fn encoder(&mut self) -> Self::Encoder {
        PreallocProstEncoder::default()
    }

    fn decoder(&mut self) -> Self::Decoder {
        ProstCodec::<T, U>::default().decoder()
    }
}

#[derive(Debug, Clone)]
pub struct PreallocProstEncoder<T> {
    marker: PhantomData<T>,
}

impl<T> Default for PreallocProstEncoder<T> {
    fn default() -> Self {
        Self {
            marker: PhantomData,
        }
    }
}

impl<T: Message> Encoder for PreallocProstEncoder<T> {
    type Item = T;
    type Error = Status;

    fn encode(&mut self, item: Self::Item, buf: &mut EncodeBuf<'_>) -> Result<(), Self::Error> {
        buf.reserve(item.encoded_len());
        item.encode(buf)
            .expect("Message only errors if not enough space");
        Ok(())
    }
}
