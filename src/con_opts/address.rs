use super::ConResult;
use rand::prelude::SliceRandom;
use std::net::{SocketAddr, ToSocketAddrs};
use std::vec::IntoIter;

/// Struct holding a resolved DSN to possibly multiple addresses.
/// Additionally holding the port and the fingerprint resulted from parsing the DSN.
#[derive(Debug, Clone)]
pub struct AddressList {
    ip_vec: IntoIter<String>,
    port: u16,
    fingerprint: Option<String>,
}

impl AddressList {
    pub fn new(hosts: Vec<String>, port: u16, fingerprint: String) -> ConResult<Self> {
        let fingerprint = match fingerprint.is_empty() {
            true => None,
            false => Some(fingerprint),
        };

        // We have to resolve hostnames to IPs
        let mut ip_vec = hosts
            .into_iter()
            .map(Self::host_to_ip_list)
            .collect::<ConResult<Vec<Vec<String>>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        ip_vec.shuffle(&mut rand::thread_rng());
        let ip_vec = ip_vec.into_iter();

        let addr_list = Self {
            ip_vec,
            port,
            fingerprint,
        };

        Ok(addr_list)
    }

    /// Takes the fingerprint out of this struct
    pub fn take_fingerprint(&mut self) -> Option<String> {
        self.fingerprint.take()
    }

    /// Resolves a hostname to a list of IP
    #[inline]
    fn host_to_ip_list(host: String) -> ConResult<Vec<String>> {
        Ok(host
            .to_socket_addrs()?
            .map(Self::sock_addr_to_ip)
            .collect::<Vec<String>>())
    }

    /// Parses a socket address to an IP
    #[inline]
    fn sock_addr_to_ip(sa: SocketAddr) -> String {
        sa.to_string().split(':').take(1).collect()
    }
}

impl Iterator for AddressList {
    type Item = (String, u16);

    fn next(&mut self) -> Option<Self::Item> {
        self.ip_vec.next().map(|addr| (addr, self.port))
    }
}
