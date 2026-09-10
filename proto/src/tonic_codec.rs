use core::marker::PhantomData;

use prost::Message;
use tonic::Status;
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};

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
    type Decoder = ProstDecoder<U>;

    fn encoder(&mut self) -> Self::Encoder {
        PreallocProstEncoder::default()
    }

    fn decoder(&mut self) -> Self::Decoder {
        ProstDecoder::default()
    }
}

#[derive(Debug, Clone)]
pub struct ProstDecoder<U> {
    marker: PhantomData<U>,
}

impl<U> Default for ProstDecoder<U> {
    fn default() -> Self {
        Self {
            marker: PhantomData,
        }
    }
}

impl<U: Message + Default> Decoder for ProstDecoder<U> {
    type Item = U;
    type Error = Status;

    fn decode(&mut self, buf: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
        Message::decode(buf)
            .map(Some)
            .map_err(|e| Status::internal(e.to_string()))
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
