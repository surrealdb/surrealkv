pub mod aol;

/*
    Vec Array implementation
*/

/// A struct `VecArray` is a generic wrapper over a Vector that can store elements of type `X`.
/// It has a pre-defined constant capacity `WIDTH`. The elements are stored in a `Vec<Option<X>>`,
/// allowing for storage to be dynamically allocated and deallocated.
#[derive(Clone)]
pub struct VecArray<X, const WIDTH: usize> {
    storage: Vec<Option<X>>,
}

/// This is an implementation of the Default trait for VecArray. It simply calls the `new` function.
impl<X, const WIDTH: usize> Default for VecArray<X, WIDTH> {
    fn default() -> Self {
        Self::new()
    }
}

impl<X, const WIDTH: usize> VecArray<X, WIDTH> {
    /// This function constructs a new VecArray with a `WIDTH` number of `Option<X>` elements.
    pub fn new() -> Self {
        Self {
            storage: Vec::with_capacity(WIDTH),
        }
    }

    /// This function adds a new element `x` to the VecArray at the first available position. If the
    /// VecArray is full, it automatically resizes to make room for more elements. It returns the
    /// position where the element was inserted.
    pub fn push(&mut self, x: X) -> usize {
        let pos = self.first_free_pos();
        self.storage[pos] = Some(x);
        pos
    }

    /// This function removes and returns the last element in the VecArray if it exists.
    pub fn pop(&mut self) -> Option<X> {
        self.last_used_pos()
            .and_then(|pos| self.storage[pos].take())
    }

    /// This function returns a reference to the last element in the VecArray, if it exists.
    pub fn last(&self) -> Option<&X> {
        self.last_used_pos()
            .and_then(|pos| self.storage[pos].as_ref())
    }

    /// This function returns the position of the last used (non-None) element in the VecArray, if it exists.
    #[inline]
    pub fn last_used_pos(&self) -> Option<usize> {
        self.storage.iter().rposition(Option::is_some)
    }

    /// This function finds the position of the first free (None) slot in the VecArray. If all slots are filled,
    /// it expands the VecArray and returns the position of the new slot.
    #[inline]
    pub fn first_free_pos(&mut self) -> usize {
        let pos = self.storage.iter().position(|x| x.is_none());
        match pos {
            Some(p) => p,
            None => {
                // No free position was found, so we add a new one.
                self.storage.push(None);
                self.storage.len() - 1
            }
        }
    }

    /// This function returns an `Option` containing a reference to the element at the given position, if it exists.
    #[inline]
    pub fn get(&self, pos: usize) -> Option<&X> {
        self.storage.get(pos).and_then(Option::as_ref)
    }

    /// This function returns an `Option` containing a mutable reference to the element at the given position, if it exists.
    #[inline]
    pub fn get_mut(&mut self, pos: usize) -> Option<&mut X> {
        self.storage.get_mut(pos).and_then(Option::as_mut)
    }

    /// This function sets the element at the given position to the provided value. If the position is out of bounds,
    /// it automatically resizes the VecArray to make room for more elements.
    #[inline]
    pub fn set(&mut self, pos: usize, x: X) {
        if pos < self.storage.len() {
            self.storage[pos] = Some(x);
        } else {
            self.storage.resize_with(pos + 1, || None);
            self.storage[pos] = Some(x);
        }
    }

    /// This function removes the element at the given position from the VecArray, returning it if it exists.
    #[inline]
    pub fn erase(&mut self, pos: usize) -> Option<X> {
        self.storage[pos].take()
    }

    /// This function clears the VecArray, removing all elements.
    pub fn clear(&mut self) {
        self.storage.clear();
    }

    /// This function checks if the VecArray is empty, returning `true` if it is and `false` otherwise.
    pub fn is_empty(&self) -> bool {
        self.storage.is_empty()
    }

    /// This function returns an iterator over the positions of all the used (non-None) elements in the VecArray.
    pub fn iter_keys(&self) -> impl DoubleEndedIterator<Item = usize> + '_ {
        self.storage.iter().enumerate().filter_map(
            |(i, x)| {
                if x.is_some() {
                    Some(i)
                } else {
                    None
                }
            },
        )
    }

    /// This function returns an iterator over pairs of positions and references to all the used (non-None) elements in the VecArray.
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = (usize, &X)> {
        self.storage
            .iter()
            .enumerate()
            .filter_map(|(i, x)| x.as_ref().map(|v| (i, v)))
    }

    /// Returns the capacity of the VecArray, which is the number of `Option<X>` elements it can hold.
    pub fn capacity(&self) -> usize {
        WIDTH
    }

    /// Returns the current number of `Option<X>` elements stored in the VecArray.
    pub fn len(&self) -> usize {
        self.storage.len()
    }

    /// Replaces the value at the specified index with a new value and returns the previous value.
    ///
    /// # Panics
    ///
    /// Panics if the index is out of bounds.
    pub fn replace(&mut self, index: usize, value: X) -> Option<X> {
        // Swap the new value with the existing one
        let old_value = self.storage[index].take();
        self.storage[index] = Some(value);
        old_value
    }
}

#[cfg(test)]
mod tests {
    use super::VecArray;

    #[test]
    fn new() {
        let v: VecArray<i32, 10> = VecArray::new();
        assert_eq!(v.storage.capacity(), 10);
    }

    #[test]
    fn push_and_pop() {
        let mut v: VecArray<i32, 10> = VecArray::new();
        let index = v.push(5);
        assert_eq!(v.get(index), Some(&5));
        assert_eq!(v.pop(), Some(5));
    }

    #[test]
    fn last() {
        let mut v: VecArray<i32, 10> = VecArray::new();
        v.push(5);
        v.push(6);
        assert_eq!(v.last(), Some(&6));
    }

    #[test]
    fn last_used_pos() {
        let mut v: VecArray<i32, 10> = VecArray::new();
        v.push(5);
        v.push(6);
        assert_eq!(v.last_used_pos(), Some(1));
    }

    #[test]
    fn first_free_pos() {
        let mut v: VecArray<i32, 10> = VecArray::new();
        v.push(5);
        assert_eq!(v.first_free_pos(), 1);
    }

    #[test]
    fn get_and_set() {
        let mut v: VecArray<i32, 10> = VecArray::new();
        v.set(5, 6);
        assert_eq!(v.get(5), Some(&6));
    }

    #[test]
    fn get_mut() {
        let mut v: VecArray<i32, 10> = VecArray::new();
        v.set(5, 6);
        if let Some(value) = v.get_mut(5) {
            *value = 7;
        }
        assert_eq!(v.get(5), Some(&7));
    }

    #[test]
    fn erase() {
        let mut v: VecArray<i32, 10> = VecArray::new();
        v.push(5);
        assert_eq!(v.erase(0), Some(5));
        assert_eq!(v.get(0), None);
    }

    #[test]
    fn clear() {
        let mut v: VecArray<i32, 10> = VecArray::new();
        v.push(5);
        v.clear();
        assert!(v.is_empty());
    }

    #[test]
    fn is_empty() {
        let mut v: VecArray<i32, 10> = VecArray::new();
        assert!(v.is_empty());
        v.push(5);
        assert!(!v.is_empty());
    }

    #[test]
    fn iter_keys() {
        let mut v: VecArray<i32, 10> = VecArray::new();
        v.push(5);
        v.push(6);
        let keys: Vec<usize> = v.iter_keys().collect();
        assert_eq!(keys, vec![0, 1]);
    }

    #[test]
    fn iter() {
        let mut v: VecArray<i32, 10> = VecArray::new();
        v.push(5);
        v.push(6);
        let values: Vec<(usize, &i32)> = v.iter().collect();
        assert_eq!(values, vec![(0, &5), (1, &6)]);
    }
}
