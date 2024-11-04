use reed_solomon_erasure::galois_8::ReedSolomon;

pub fn encode(payload: &Vec<u8>, shard_count: usize, parity_count: usize) -> Vec<Vec<u8>> {
    // Use erasure coding for the message
    let mut bytes_message = payload.clone();

    // Pad message if not multiplication of shard_count
    let shard_size = (bytes_message.len() + shard_count - 1) / shard_count;
    let total_len = shard_size * shard_count;
    bytes_message.resize(total_len, 0);

    let mut shard_data: Vec<Vec<u8>> = Vec::with_capacity(shard_count + parity_count);
    for i in 0..(shard_count) {
        let start = i * shard_size;
        let end = start + shard_size;
        shard_data.push(bytes_message[start..end].to_vec());
    }
    for _ in 0..(parity_count) {
        let array: Vec<u8> = (0..shard_size).map(|_| 0).collect();
        shard_data.push(array);
    }

    let rs = ReedSolomon::new(shard_count, parity_count).unwrap();
    rs.encode(&mut shard_data).unwrap();

    shard_data
}

pub fn reconstruct(shards: &mut Vec<Option<Vec<u8>>>, shard_count: usize, parity_count: usize) {
    let rs = ReedSolomon::new(shard_count, parity_count).unwrap();
    rs.reconstruct(shards).unwrap();
}
