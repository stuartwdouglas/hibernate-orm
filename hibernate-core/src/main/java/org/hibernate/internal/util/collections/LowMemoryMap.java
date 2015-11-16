package org.hibernate.internal.util.collections;

import java.io.Serializable;
import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;


/**
 * A map implementation that uses less memory than the default JDK map. This is achieved by storing keys and values
 * in the same same, without the use of an Entry as a holder.
 *
 * It also provides a fast iterator concept that allows for iteration over the map without the need to allocate an
 * iterator.
 *
 * For now this map has a hard coded 50% load factor.
 *
 * @author Stuart Douglas
 */
public class LowMemoryMap<K, V> implements Map<K, V>, Serializable {

	static final int MAXIMUM_CAPACITY = 1 << 30;

	private Object[] table;
	private int tableLength; //table.length / 2, we store explicitly to avoid a bit shift
	private int size;

	private transient EntrySet entrySet;
	private transient KeySet keySet;
	private transient Values values;

	public LowMemoryMap() {
		//we overload size to contain the initial allocation size
		size = 16;
	}

	public LowMemoryMap(int initialCapacity) {
		//we overload size to contain the initial allocation size
		size = tableSizeFor(initialCapacity);
	}

	@Override
	public int size() {
		return table == null ? 0 : size;
	}

	@Override
	public boolean isEmpty() {
		return size() == 0;
	}

	@Override
	public boolean containsKey(Object key) {
		if ( table == null || size == 0 ) {
			return false;
		}
		if ( key == null ) {
			return false;
		}
		return table[pos(key)] != null;
	}

	@Override
	public boolean containsValue(Object value) {
		if ( table == null || size == 0 ) {
			return false;
		}
		int fi = fastIterate();
		while ( fi != -1 ) {
			if( fastIterateValue( fi ).equals(value)) {
				return true;
			}
			fi = fastIterateNext(fi);
		}
		return false;
	}

	@Override
	public V get(Object key) {
		if ( table == null || size == 0 ) {
			return null;
		}
		int pos = pos(key);
		Object v = table[pos];
		if( v == null ) {
			return null;
		}
		if( v instanceof EntryImpl ) {
			return ((EntryImpl<K, V>) v).getValue();
		}
		return (V) table[pos + 1];
	}

	@Override
	public V put(K key, V value) {
		if( key == null ) {
			throw new NullPointerException();
		}
		if ( table == null || size == 0 ) {
			putNewEntry(key, value);
			return null;
		}
		int pos = pos(key);
		Object k = table[pos];
		if( k == null ) {
			putNewEntry(key, value);
			return null;
		}
		if( k instanceof EntryImpl ) {
			EntryImpl<K, V> entry = (EntryImpl<K, V>) k;
			V old = entry.getValue();
			entry.setValue(value);
			return old;
		} else {
			V old = (V) table[pos + 1];
			table[pos + 1] = value;
			return old;
		}
	}

	private void putNewEntry(K key, V value) {
		resizeIfRequired();
		int pos = pos(key);
		table[pos] = key;
		table[pos + 1] = value;
		size++;
	}

	private void resizeIfRequired() {
		if(table == null) {
			table = new Object[size * 2];
			tableLength = size;
			size = 0;
		} else {
			int newSize = size + 1;
			if(newSize == MAXIMUM_CAPACITY) {
				throw new RuntimeException("Map is full"); // pathalogical case
			}
			if(tableLength == MAXIMUM_CAPACITY) {
				return;
			}
			if ( newSize * 2 > tableLength ) {
				int newTableLength = (tableLength << 1);
				Object[] newTable = new Object[newTableLength * 2];
				Object[] old = table;
				this.tableLength = newTableLength;
				this.table = newTable;
				for(int i = 0; i < old.length; i +=2 ) {
					Object key = old[i];
					if( key != null ) {
						if(key instanceof EntryImpl ) {
							int pos = pos(((EntryImpl) key).key);
							table[pos] = key;
						} else {
							int pos = pos(key);
							table[pos] = key;
							table[pos + 1] = old[i + 1];
						}
					}
				}
			}
		}
	}

	@Override
	public V remove(Object key) {
		if( table == null ) {
			return null;
		}
		int pos = pos(key);
		return removeInternal(pos);
	}

	private V removeInternal(int pos) {
		Object k = table[pos];
		if( k == null ) {
			return null;
		}
		size--;
		V ret;
		if ( k instanceof EntryImpl) {
			ret = ((EntryImpl<K, V>) k).getValue();
		} else {
			ret = (V) table[pos+1];
		}
		table[pos] = null;
		table[pos + 1] = null;
		do {
			pos += 2;
			if( pos >= table.length ) {
				pos = 0;
			}
			k = table[pos];
			if( k == null ) {
				return ret;
			}
			Object actualKey;
			if( k instanceof EntryImpl) {
				actualKey = ((EntryImpl) k).getKey();
			} else {
				actualKey = k;
			}
			int expectedPos = pos(actualKey);
			if( expectedPos != pos ) {
				table[expectedPos] = table[pos];
				table[expectedPos + 1] = table[pos + 1];
				table[pos] = null;
				table[pos + 1] = null;
			}
		} while (true);
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		if(m instanceof LowMemoryMap) {
			LowMemoryMap<K, V> lowMemoryMap = (LowMemoryMap<K, V>) m;
			int fi = lowMemoryMap.fastIterate();
			while ( fi != -1 ) {
				put( lowMemoryMap.fastIterateKey(fi), lowMemoryMap.fastIterateValue(fi) );
				fi = lowMemoryMap.fastIterateNext(fi);
			}
		} else {
			for( Entry<? extends K, ? extends V> entry : m.entrySet() ) {
				put(entry.getKey(), entry.getValue());
			}
		}
	}

	@Override
	public void clear() {
		if( table == null ) {
			return;
		}
		Arrays.fill(table, null);
		size = 0;
	}

	@Override
	public Set<K> keySet() {
		if ( keySet == null ) {
			keySet = new KeySet();
		}
		return keySet;
	}

	@Override
	public Collection<V> values() {
		if ( values == null ) {
			values = new Values();
		}
		return values;
	}

	@Override
	public Set<Entry<K, V>> entrySet() {
		if ( entrySet == null) {
			entrySet = new EntrySet();
		}
		return entrySet;
	}

	public int fastIterate() {
		if( table == null ) {
			return -1;
		}
		for( int i = 0; i < table.length; i+=2 ) {
			if( table[i] != null ) {
				return i;
			}
		}
		return -1;
	}

	public int fastIterateNext(int current) {
		for( int i = current + 2; i < table.length; i+=2 ) {
			if( table[i] != null ) {
				return i;
			}
		}
		return -1;
	}

	public K fastIterateKey(int current) {
		Object o = table[current];
		if(o instanceof EntryImpl) {
			return ((EntryImpl<K, V>) o).getKey();
		}
		return (K) o;
	}

	public V fastIterateValue(int current) {
		Object o = table[current];
		if(o instanceof EntryImpl) {
			return ((EntryImpl<K, V>) o).getValue();
		}
		return (V) table[current + 1];
	}

	public V fastIterateRemove(int current) {
		return removeInternal(current);
	}

	private int pos( Object key ) {
		int hash = hash( key );
		int pos = (hash & (tableLength - 1)) << 1;
		Object k = table[pos];
		while ( k != null ) {
			if ( k instanceof EntryImpl ) {
				if( equalsHelper(((EntryImpl) k).getKey(), key) ) {
					return pos;
				}
			} else if( equalsHelper(k, key) ) {
				return pos;
			}
			pos += 2;
			if ( pos >= table.length ) {
				pos = 0;
			}
			k = table[pos];
		}
		return pos;
	}

	static final int hash(Object key) {
		int h;
		return (h = key.hashCode()) ^ (h >>> 16);
	}

	/**
	 * Compares the specified object with this map for equality.  Returns
	 * <tt>true</tt> if the given object is also a map and the two maps
	 * represent the same mappings.  More formally, two maps <tt>m1</tt> and
	 * <tt>m2</tt> represent the same mappings if
	 * <tt>m1.entrySet().equals(m2.entrySet())</tt>.  This ensures that the
	 * <tt>equals</tt> method works properly across different implementations
	 * of the <tt>Map</tt> interface.
	 *
	 * @implSpec
	 * This implementation first checks if the specified object is this map;
	 * if so it returns <tt>true</tt>.  Then, it checks if the specified
	 * object is a map whose size is identical to the size of this map; if
	 * not, it returns <tt>false</tt>.  If so, it iterates over this map's
	 * <tt>entrySet</tt> collection, and checks that the specified map
	 * contains each mapping that this map contains.  If the specified map
	 * fails to contain such a mapping, <tt>false</tt> is returned.  If the
	 * iteration completes, <tt>true</tt> is returned.
	 *
	 * @param o object to be compared for equality with this map
	 * @return <tt>true</tt> if the specified object is equal to this map
	 */
	public boolean equals(Object o) {
		if (o == this)
			return true;

		if (!(o instanceof Map))
			return false;
		Map<?,?> m = (Map<?,?>) o;
		if (m.size() != size())
			return false;

		try {
			Iterator<Entry<K,V>> i = entrySet().iterator();
			while (i.hasNext()) {
				Entry<K,V> e = i.next();
				K key = e.getKey();
				V value = e.getValue();
				if (value == null) {
					if (!(m.get(key)==null && m.containsKey(key)))
						return false;
				} else {
					if (!value.equals(m.get(key)))
						return false;
				}
			}
		} catch (ClassCastException unused) {
			return false;
		} catch (NullPointerException unused) {
			return false;
		}

		return true;
	}

	/**
	 * Returns the hash code value for this map.  The hash code of a map is
	 * defined to be the sum of the hash codes of each entry in the map's
	 * <tt>entrySet()</tt> view.  This ensures that <tt>m1.equals(m2)</tt>
	 * implies that <tt>m1.hashCode()==m2.hashCode()</tt> for any two maps
	 * <tt>m1</tt> and <tt>m2</tt>, as required by the general contract of
	 * {@link Object#hashCode}.
	 *
	 * @implSpec
	 * This implementation iterates over <tt>entrySet()</tt>, calling
	 * {@link Map.Entry#hashCode hashCode()} on each element (entry) in the
	 * set, and adding up the results.
	 *
	 * @return the hash code value for this map
	 * @see Map.Entry#hashCode()
	 * @see Object#equals(Object)
	 * @see Set#equals(Object)
	 */
	public int hashCode() {
		int h = 0;
		Iterator<Entry<K,V>> i = entrySet().iterator();
		while (i.hasNext())
			h += i.next().hashCode();
		return h;
	}


	/**
	 * Returns a string representation of this map.  The string representation
	 * consists of a list of key-value mappings in the order returned by the
	 * map's <tt>entrySet</tt> view's iterator, enclosed in braces
	 * (<tt>"{}"</tt>).  Adjacent mappings are separated by the characters
	 * <tt>", "</tt> (comma and space).  Each key-value mapping is rendered as
	 * the key followed by an equals sign (<tt>"="</tt>) followed by the
	 * associated value.  Keys and values are converted to strings as by
	 * {@link String#valueOf(Object)}.
	 *
	 * @return a string representation of this map
	 */
	public String toString() {
		Iterator<Entry<K,V>> i = entrySet().iterator();
		if (! i.hasNext())
			return "{}";

		StringBuilder sb = new StringBuilder();
		sb.append('{');
		for (;;) {
			Entry<K,V> e = i.next();
			K key = e.getKey();
			V value = e.getValue();
			sb.append(key   == this ? "(this Map)" : key);
			sb.append('=');
			sb.append(value == this ? "(this Map)" : value);
			if (! i.hasNext())
				return sb.append('}').toString();
			sb.append(',').append(' ');
		}
	}

	/**
	 * Returns a power of two size for the given target capacity.
	 */
	static final int tableSizeFor(int cap) {
		int n = cap - 1;
		n |= n >>> 1;
		n |= n >>> 2;
		n |= n >>> 4;
		n |= n >>> 8;
		n |= n >>> 16;
		return (n < 0) ? 1 : (n >= MAXIMUM_CAPACITY) ? MAXIMUM_CAPACITY : n + 1;
	}

	private class EntrySet extends AbstractSet<Entry<K, V>> {

		@Override
		public Iterator<Entry<K, V>> iterator() {
			return new Iterator<Entry<K, V>>() {
				int it = fastIterate();
				int prev = -1;

				@Override
				public boolean hasNext() {
					return it != -1;
				}

				@Override
				public Entry<K, V> next() {
					if( it == -1 ) {
						throw new NoSuchElementException();
					}
					prev = it;
					Object k = table[it];
					if ( k instanceof EntryImpl ) {
						it = fastIterateNext(it);
						return (Entry<K, V>) k;
					}
					Object val = table[it + 1];
					EntryImpl entry = new EntryImpl(k ,val);
					table[it] = entry;
					table[it + 1] = null;
					it = fastIterateNext(it);
					return entry;
				}

				@Override
				public void remove() {
					if(prev == -1) {
						throw new NoSuchElementException();
					}
					fastIterateRemove(prev);
				}
			};
		}

		@Override
		public int size() {
			return LowMemoryMap.this.size();
		}

		@Override
		public void clear() {
			LowMemoryMap.this.clear();
		}

		@Override
		public boolean remove(Object o) {
			for( int i = 0; i < table.length; i += 2 ) {
				if ( table[i] == o ) {
					fastIterateRemove(i);
					return true;
				}
			}
			return false;
		}

		@Override
		public boolean contains(Object o) {
			for( int i = 0; i < table.length; i += 2 ) {
				if ( table[i] == o ) {
					return true;
				}
			}
			return false;
		}
	}

	private class KeySet extends AbstractSet<K> {

		@Override
		public Iterator<K> iterator() {
			return new Iterator<K>() {
				int prev = -1;
				int it = fastIterate();

				@Override
				public boolean hasNext() {
					return it != -1;
				}

				@Override
				public K next() {
					if( it == -1 ) {
						throw new NoSuchElementException();
					}
					prev = it;
					Object k = table[it];
					if ( k instanceof EntryImpl ) {
						it = fastIterateNext(it);
						return ((Entry<K, V>) k).getKey();
					}
					Object val = table[it];
					it = fastIterateNext(it);
					return (K) val;
				}

				@Override
				public void remove() {
					if(prev == -1) {
						throw new NoSuchElementException();
					}
					fastIterateRemove(prev);
				}
			};
		}

		@Override
		public int size() {
			return LowMemoryMap.this.size();
		}

		@Override
		public void clear() {
			LowMemoryMap.this.clear();
		}

		@Override
		public boolean remove(Object o) {
			return LowMemoryMap.this.remove(o) != null;
		}

		@Override
		public boolean contains(Object o) {
			return table[pos(o)] != null;
		}
	}

	private class Values extends AbstractCollection<V> {

		@Override
		public Iterator<V> iterator() {
			return new Iterator<V>() {
				int prev = -1;
				int it = fastIterate();

				@Override
				public boolean hasNext() {
					return it != -1;
				}

				@Override
				public V next() {
					if( it == -1 ) {
						throw new NoSuchElementException();
					}
					prev = it;
					Object k = table[it];
					if ( k instanceof EntryImpl ) {
						it = fastIterateNext(it);
						return ((Entry<K, V>) k).getValue();
					}
					Object val = table[it + 1];
					it = fastIterateNext(it);
					return (V) val;
				}

				@Override
				public void remove() {
					if(prev == -1) {
						throw new NoSuchElementException();
					}
					fastIterateRemove(prev);
				}
			};
		}

		@Override
		public int size() {
			return LowMemoryMap.this.size();
		}

		@Override
		public void clear() {
			LowMemoryMap.this.clear();
		}

		@Override
		public boolean remove(Object o) {
			for( int i = 0; i < table.length; i += 2 ) {
				Object k = table[i];
				if( k instanceof EntryImpl ) {
					if ( equalsHelper(((EntryImpl)k).getValue(), o) ) {
						fastIterateRemove(i);
						return true;
					}
				} else {
					if ( equalsHelper(table[i + 1], o) ) {
						fastIterateRemove(i);
						return true;
					}
				}
			}
			return false;
		}

		@Override
		public boolean contains(Object o) {
			for( int i = 0; i < table.length; i += 2 ) {
				Object k = table[i];
				if( k instanceof EntryImpl ) {
					if ( equalsHelper(((EntryImpl)k).getValue(), o) ) {
						return true;
					}
				} else {
					if ( equalsHelper(table[i + 1], o) ) {
						return true;
					}
				}
			}
			return false;
		}
	}

	static boolean equalsHelper(Object o1, Object o2) {
		if( o1 == o2 ) {
			return true;
		}
		if( o1 == null && o2 == null ) {
			return true;
		}
		if( o1 == null || o2 == null ) {
			return false;
		}
		return o1.equals(o2);
	}

	private static class EntryImpl<K, V> implements Entry<K, V> {

		private final K key;
		private V value;

		private EntryImpl(K key, V value) {
			this.key = key;
			this.value = value;
		}

		@Override
		public K getKey() {
			return key;
		}

		@Override
		public V getValue() {
			return value;
		}

		@Override
		public V setValue(V value) {
			V ret = this.value;
			this.value = value;
			return ret;
		}
	}
}

