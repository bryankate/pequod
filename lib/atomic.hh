
#ifndef ATOMIC_HH_
#define ATOMIC_HH_

namespace gstore {

template <int SIZE, typename BARRIER> struct sized_compiler_operations;

template <typename B> struct sized_compiler_operations<1, B> {
    typedef uint8_t type;
    static inline type xchg(type *object, type new_value) {
	asm volatile("xchgb %0,%1"
		     : "+q" (new_value), "+m" (*object));
	B()();
	return new_value;
    }
    static inline type val_cmpxchg(type *object, type expected, type desired) {
#if __x86__ && (PREFER_X86 || !HAVE___SYNC_VAL_COMPARE_AND_SWAP)
	asm volatile("lock; cmpxchgb %2,%1"
		     : "+a" (expected), "+m" (*object)
		     : "r" (desired) : "cc");
	B()();
	return expected;
#else
	return __sync_val_compare_and_swap(object, expected, desired);
#endif
    }
    static inline bool bool_cmpxchg(type *object, type expected, type desired) {
#if HAVE___SYNC_BOOL_COMPARE_AND_SWAP && ALLOW___SYNC_BUILTINS
	return __sync_bool_compare_and_swap(object, expected, desired);
#else
	bool result;
	asm volatile("lock; cmpxchgb %3,%1; sete %b2"
		     : "+a" (expected), "+m" (*object), "=q" (result)
		     : "q" (desired) : "cc");
	B()();
	return result;
#endif
    }
    static inline type fetch_and_add(type *object, type addend) {
#if __x86__ && (PREFER_X86 || !HAVE___SYNC_FETCH_AND_ADD)
	asm volatile("lock; xaddb %0,%1"
		     : "+q" (addend), "+m" (*object) : : "cc");
	B()();
	return addend;
#else
	return __sync_fetch_and_add(object, addend);
#endif
    }
};

template <typename B> struct sized_compiler_operations<2, B> {
    typedef uint16_t type;
    static inline type xchg(type *object, type new_value) {
	asm volatile("xchgw %0,%1"
		     : "+r" (new_value), "+m" (*object));
	B()();
	return new_value;
    }
    static inline type val_cmpxchg(type *object, type expected, type desired) {
#if __x86__ && (PREFER_X86 || !HAVE___SYNC_VAL_COMPARE_AND_SWAP)
	asm volatile("lock; cmpxchgw %2,%1"
		     : "+a" (expected), "+m" (*object)
		     : "r" (desired) : "cc");
	B()();
	return expected;
#else
	return __sync_val_compare_and_swap(object, expected, desired);
#endif
    }
    static inline bool bool_cmpxchg(type *object, type expected, type desired) {
#if HAVE___SYNC_BOOL_COMPARE_AND_SWAP && ALLOW___SYNC_BUILTINS
	return __sync_bool_compare_and_swap(object, expected, desired);
#else
	bool result;
	asm volatile("lock; cmpxchgw %3,%1; sete %b2"
		     : "+a" (expected), "+m" (*object), "=q" (result)
		     : "r" (desired) : "cc");
	B()();
	return result;
#endif
    }
    static inline type fetch_and_add(type *object, type addend) {
#if __x86__ && (PREFER_X86 || !HAVE___SYNC_FETCH_AND_ADD)
	asm volatile("lock; xaddw %0,%1"
		     : "+r" (addend), "+m" (*object) : : "cc");
	B()();
	return addend;
#else
	return __sync_fetch_and_add(object, addend);
#endif
    }
};

template <typename B> struct sized_compiler_operations<4, B> {
    typedef uint32_t type;
    static inline type xchg(type *object, type new_value) {
	asm volatile("xchgl %0,%1"
		     : "+r" (new_value), "+m" (*object));
	B()();
	return new_value;
    }
    static inline type val_cmpxchg(type *object, type expected, type desired) {
#if __x86__ && (PREFER_X86 || !HAVE___SYNC_VAL_COMPARE_AND_SWAP)
	asm volatile("lock; cmpxchgl %2,%1"
		     : "+a" (expected), "+m" (*object)
		     : "r" (desired) : "cc");
	B()();
	return expected;
#else
	return __sync_val_compare_and_swap(object, expected, desired);
#endif
    }
    static inline bool bool_cmpxchg(type *object, type expected, type desired) {
#if HAVE___SYNC_BOOL_COMPARE_AND_SWAP && ALLOW___SYNC_BUILTINS
	return __sync_bool_compare_and_swap(object, expected, desired);
#else
	bool result;
	asm volatile("lock; cmpxchgl %3,%1; sete %b2"
		     : "+a" (expected), "+m" (*object), "=q" (result)
		     : "r" (desired) : "cc");
	B()();
	return result;
#endif
    }
    static inline type fetch_and_add(type *object, type addend) {
#if __x86__ && (PREFER_X86 || !HAVE___SYNC_FETCH_AND_ADD)
	asm volatile("lock; xaddl %0,%1"
		     : "+r" (addend), "+m" (*object) : : "cc");
	B()();
	return addend;
#else
	return __sync_fetch_and_add(object, addend);
#endif
    }
};

template <typename B> struct sized_compiler_operations<8, B> {
    typedef uint64_t type;
#if __x86_64__
    static inline type xchg(type *object, type new_value) {
	asm volatile("xchgq %0,%1"
		     : "+r" (new_value), "+m" (*object));
	B()();
	return new_value;
    }
#endif
    static inline type val_cmpxchg(type *object, type expected, type desired) {
#if __x86_64__ && (PREFER_X86 || !HAVE___SYNC_VAL_COMPARE_AND_SWAP)
	asm volatile("lock; cmpxchgq %2,%1"
		     : "+a" (expected), "+m" (*object)
		     : "r" (desired) : "cc");
	B()();
	return expected;
#elif __i386__ && (PREFER_X86 || !HAVE___SYNC_VAL_COMPARE_AND_SWAP)
	uint32_t expected_low(expected), expected_high(expected >> 32),
	    desired_low(desired), desired_high(desired >> 32);
	asm volatile("lock; cmpxchg8b %2"
		     : "+a" (expected_low), "+d" (expected_high), "+m" (*object)
		     : "b" (desired_low), "c" (desired_high) : "cc");
	B()();
	return ((uint64_t) expected_high << 32) | expected_low;
#elif HAVE___SYNC_VAL_COMPARE_AND_SWAP
	return __sync_val_compare_and_swap(object, expected, desired);
#endif
    }
    static inline bool bool_cmpxchg(type *object, type expected, type desired) {
#if HAVE___SYNC_BOOL_COMPARE_AND_SWAP && ALLOW___SYNC_BUILTINS
	return __sync_bool_compare_and_swap(object, expected, desired);
#elif __x86_64__
	bool result;
	asm volatile("lock; cmpxchgq %3,%1; sete %b2"
		     : "+a" (expected), "+m" (*object), "=q" (result)
		     : "r" (desired) : "cc");
	B()();
	return result;
#else
	uint32_t expected_low(expected), expected_high(expected >> 32),
	    desired_low(desired), desired_high(desired >> 32);
	bool result;
	asm volatile("lock; cmpxchg8b %2; sete %b4"
		     : "+a" (expected_low), "+d" (expected_high),
		       "+m" (*object), "=q" (result)
		     : "b" (desired_low), "c" (desired_high) : "cc");
	B()();
	return result;
#endif
    }
#if __x86_64__ || HAVE___SYNC_FETCH_AND_ADD
    static inline type fetch_and_add(type *object, type addend) {
# if __x86_64__ && (PREFER_X86 || !HAVE___SYNC_FETCH_AND_ADD)
	asm volatile("lock; xaddq %0,%1"
		     : "+r" (addend), "+m" (*object) : : "cc");
	B()();
	return addend;
# else
	return __sync_fetch_and_add(object, addend);
# endif
    }
#endif
};

template<typename T>
inline T xchg(T *object, T new_value) {
    typedef sized_compiler_operations<sizeof(T), fence_function> sco_t;
    typedef typename sco_t::type type;
    return (T) sco_t::xchg((type *) object, (type) new_value);
}

inline int8_t xchg(int8_t *object, int new_value) {
    return xchg(object, (int8_t) new_value);
}
inline uint8_t xchg(uint8_t *object, int new_value) {
    return xchg(object, (uint8_t) new_value);
}
inline int16_t xchg(int16_t *object, int new_value) {
    return xchg(object, (int16_t) new_value);
}
inline uint16_t xchg(uint16_t *object, int new_value) {
    return xchg(object, (uint16_t) new_value);
}
inline unsigned xchg(unsigned *object, int new_value) {
    return xchg(object, (unsigned) new_value);
}

/** @brief Atomic compare and exchange. Return actual old value.
 * @param object pointer to memory value
 * @param expected old value
 * @param desired new value
 * @return actual old value
 *
 * Acts like an atomic version of:
 * @code
 * T actual(*object);
 * if (actual == expected)
 *    *object = desired;
 * return actual;
 * @endcode */
template <typename T>
inline T cmpxchg(T *object, T expected, T desired) {
    typedef sized_compiler_operations<sizeof(T), fence_function> sco_t;
    typedef typename sco_t::type type;
    return (T) sco_t::val_cmpxchg((type *) object, (type) expected, (type) desired);
}

inline unsigned cmpxchg(unsigned *object, int expected, int desired) {
    return cmpxchg(object, unsigned(expected), unsigned(desired));
}

/** @brief Atomic compare and exchange. Return true iff swap succeeds.
 * @param object pointer to memory value
 * @param expected old value
 * @param desired new value
 * @return true if swap succeeded, false otherwise
 *
 * Acts like an atomic version of:
 * @code
 * T actual(*object);
 * if (actual == expected) {
 *    *object = desired;
 *    return true;
 * } else
 *    return false;
 * @endcode */
template <typename T>
inline bool bool_cmpxchg(T *object, T expected, T desired) {
    typedef sized_compiler_operations<sizeof(T), fence_function> sco_t;
    typedef typename sco_t::type type;
    return sco_t::bool_cmpxchg((type *) object, (type) expected, (type) desired);
}

inline bool bool_cmpxchg(uint8_t *object, int expected, int desired) {
    return bool_cmpxchg(object, uint8_t(expected), uint8_t(desired));
}
inline bool bool_cmpxchg(unsigned *object, int expected, int desired) {
    return bool_cmpxchg(object, unsigned(expected), unsigned(desired));
}

/** @brief Atomic fetch-and-add. Return the old value.
 * @param object pointer to integer
 * @param addend value to add
 * @return old value */
template <typename T>
inline T fetch_and_add(T *object, T addend) {
    typedef sized_compiler_operations<sizeof(T), fence_function> sco_t;
    typedef typename sco_t::type type;
    return (T) sco_t::fetch_and_add((type *) object, (type) addend);
}

template <typename T>
inline T *fetch_and_add(T **object, int addend) {
    typedef sized_compiler_operations<sizeof(T *), fence_function> sco_t;
    typedef typename sco_t::type type;
    return (T *) sco_t::fetch_and_add((type *) object, (type) (addend * sizeof(T)));
}

inline int8_t fetch_and_add(int8_t *object, int addend) {
    return fetch_and_add(object, int8_t(addend));
}
inline uint8_t fetch_and_add(uint8_t *object, int addend) {
    return fetch_and_add(object, uint8_t(addend));
}
inline int16_t fetch_and_add(int16_t *object, int addend) {
    return fetch_and_add(object, int16_t(addend));
}
inline uint16_t fetch_and_add(uint16_t *object, int addend) {
    return fetch_and_add(object, uint16_t(addend));
}
inline unsigned fetch_and_add(unsigned *object, int addend) {
    return fetch_and_add(object, unsigned(addend));
}
inline unsigned long fetch_and_add(unsigned long *object, int addend) {
    return fetch_and_add(object, (unsigned long)(addend));
}

}

#endif
