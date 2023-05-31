use std::cmp::max;
use std::net::IpAddr;
use std::sync::mpsc;
use bytes::Bytes;
use crc32fast::Hasher;

pub fn get_local_ip() -> Result<IpAddr, std::io::Error> {
    let ip = std::env::var("DATANODE_IP");
    if ip.is_ok() {
        Ok(ip.unwrap().parse().unwrap())
    } else {
        let socket = std::net::UdpSocket::bind("0.0.0.0:0")?;
        socket.connect("8.8.8.8:80")?;
        let local_addr = socket.local_addr()?;
        Ok(local_addr.ip())
    }
}

const LENGTH_PER_CRC: usize = 4 * 1024;
pub fn get_crc(bytes: &Bytes) -> i64 {
    let mut crc32 = Hasher::new();
    let offset = 0;
    let length = bytes.len();
    let crc_buffer = &bytes[offset..(offset + length)];

    for i in (0..length).step_by(LENGTH_PER_CRC) {
        let len = std::cmp::min(LENGTH_PER_CRC, length - i);
        let crc_slice = &crc_buffer[i..(i + len)];

        crc32.update(crc_slice);
    }

    crc32.finalize() as i64
}

#[cfg(test)]
mod test {
    use bytes::Bytes;
    use crate::util::get_crc;

    #[test]
    fn crc_test() {
        let data = Bytes::from("hello world! hello china!");
        let crc_value = get_crc(&data);
        // This value is the same with java's implementation
        assert_eq!(3871485936, crc_value);
    }

    #[test]
    fn drop_test() {
    }
}