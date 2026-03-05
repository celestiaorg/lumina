/// Map actual row index to padded tree position.
pub fn map_index_to_tree_position(index: usize, k: usize) -> usize {
    let k_padded = k.next_power_of_two();
    if index < k {
        index
    } else {
        k_padded + (index - k)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_map_index() {
        // K=5, K_padded=8
        assert_eq!(map_index_to_tree_position(0, 5), 0);
        assert_eq!(map_index_to_tree_position(4, 5), 4);
        assert_eq!(map_index_to_tree_position(5, 5), 8); // First parity row
        assert_eq!(map_index_to_tree_position(6, 5), 9);
    }
}
