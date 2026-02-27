use crate::field::GF128;

/// Build padded row array for Merkle tree
pub fn build_padded_row_array(rows: &[Vec<u8>], k: usize, n: usize) -> Vec<Vec<u8>> {
    let k_padded = k.next_power_of_two();
    let total_padded = (k_padded + n).next_power_of_two();
    let row_size = rows[0].len();

    let mut padded_rows = Vec::with_capacity(total_padded);
    let zero_row = vec![0u8; row_size];

    // Original rows (clone needed - different from source)
    padded_rows.extend_from_slice(&rows[0..k]);

    // Padding for positions K to K_padded (reuse zero_row reference)
    for _ in k..k_padded {
        padded_rows.push(zero_row.clone());
    }

    // Extended rows
    padded_rows.extend_from_slice(&rows[k..k + n]);

    // Padding for positions K_padded+N to total_padded (reuse zero_row reference)
    for _ in (k_padded + n)..total_padded {
        padded_rows.push(zero_row.clone());
    }

    padded_rows
}

/// Build padded RLC array for Merkle tree
pub fn build_padded_rlc_array(rlc_orig: &[GF128], k: usize) -> Vec<Vec<u8>> {
    let k_padded = k.next_power_of_two();
    let mut padded_rlc_leaves = Vec::with_capacity(k_padded);
    let zero_rlc = vec![0u8; 16];

    // Original K RLCs
    for i in 0..k {
        padded_rlc_leaves.push(rlc_orig[i].to_bytes().to_vec());
    }

    // Padding K to K_padded
    for _ in k..k_padded {
        padded_rlc_leaves.push(zero_rlc.clone());
    }

    padded_rlc_leaves
}

/// Map actual index to padded tree position
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
        assert_eq!(map_index_to_tree_position(5, 5), 8); // First extended row
        assert_eq!(map_index_to_tree_position(6, 5), 9);
    }
}
