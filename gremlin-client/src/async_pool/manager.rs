use std::future::Future;

use super::state::ConnectionState;

pub trait Manager: 'static + Send + Sync {
    type Connection: Send;
    type ConnectOption: Send;
    type Error: Send + std::error::Error;

    fn connect(&self) -> impl Future<Output = Result<Self::Connection, Self::Error>> + Send;

    fn should_recycle(
        &self,
        conn: &Self::Connection,
        status: &ConnectionState,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn check(
        &self,
        conn: Self::Connection,
    ) -> impl Future<Output = Result<Self::Connection, Self::Error>> + Send;

    fn set_connect_option(&self, options: Self::ConnectOption);

    fn get_connect_option(&self) -> Self::ConnectOption;
}
