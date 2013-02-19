#ifndef GSTORE_INTERVAL_TREE_HH
#define GSTORE_INTERVAL_TREE_HH 1
#include "compiler.hh"
#include "rb.hh"
#include "interval.hh"

template <typename T>
struct rbintervalvalue : public T {
    typedef typename T::endpoint_type endpoint_type;
    endpoint_type max_child_iend_;

    inline rbintervalvalue(const T &x)
	: T(x) {
	max_child_iend_ = this->iend();
    }
    inline rbintervalvalue(T &&x) noexcept
	: T(std::move(x)) {
	max_child_iend_ = this->iend();
    }
};

struct interval_comparator {
    template <typename A, typename B>
    inline int operator()(const A &a, const B &b) const {
	int cmp = default_compare(a.ibegin(), b.ibegin());
	return cmp ? cmp : default_compare(a.iend(), b.iend());
    }
};

struct interval_rb_reshaper {
    template <typename T>
    inline void operator()(T* n) {
	n->set_subtree_iend(n->iend());
	for (int i = 0; i < 2; ++i)
	    if (T* x = n->rblinks_.c_[i].node())
		if (n->subtree_iend() < x->subtree_iend())
		    n->set_subtree_iend(x->subtree_iend());
    }
};

template <typename T>
class interval_tree {
  public:
    typedef T value_type;
    typedef typename T::endpoint_type endpoint_type;

    inline interval_tree();

    template <typename X> inline value_type* find(const X &i);
    template <typename X> inline const value_type* find(const X &i) const;

    inline void insert(value_type* x);

    inline void erase(value_type* x);
    inline void erase_and_dispose(value_type* x);
    template <typename Dispose> inline void erase_and_dispose(value_type* x, Dispose d);

    template <typename F>
    inline size_t visit_contains(const endpoint_type &x, const F &f);
    template <typename F>
    inline size_t visit_contains(const endpoint_type &x, F &f);
    template <typename I, typename F>
    inline size_t visit_contains(const I &x, const F &f);
    template <typename I, typename F>
    inline size_t visit_contains(const I &x, F &f);

    template <typename F>
    inline size_t visit_overlaps(const endpoint_type &x, const F &f);
    template <typename F>
    inline size_t visit_overlaps(const endpoint_type &x, F &f);
    template <typename I, typename F>
    inline size_t visit_overlaps(const I &x, const F &f);
    template <typename I, typename F>
    inline size_t visit_overlaps(const I &x, F &f);

    template <typename TT> friend std::ostream &operator<<(std::ostream &s, const interval_tree<TT> &x);

  private:
    rbtree<T, interval_comparator, interval_rb_reshaper> t_;

    template <typename F>
    static size_t visit_contains(value_type *node, const endpoint_type &x, F &f);
    template <typename I, typename F>
    static size_t visit_contains(value_type *node, const I &x, F &f);
    template <typename I, typename F>
    static size_t visit_overlaps(value_type *node, const I &x, F &f);
};

template <typename T>
inline interval_tree<T>::interval_tree() {
}

template <typename T> template <typename X>
inline T* interval_tree<T>::find(const X &i) {
    return t_.find(i);
}

template <typename T> template <typename X>
inline const T* interval_tree<T>::find(const X &i) const {
    return t_.find(i);
}

template <typename T>
inline void interval_tree<T>::insert(value_type* node) {
    return t_.insert(node);
}

template <typename T>
inline void interval_tree<T>::erase(T* node) {
    t_.erase(node);
}

template <typename T>
inline void interval_tree<T>::erase_and_dispose(T* node) {
    t_.erase_and_dispose(node);
}

template <typename T> template <typename Disposer>
inline void interval_tree<T>::erase_and_dispose(T* node, Disposer d) {
    t_.erase_and_dispose(node, d);
}

template <typename T> template <typename F>
size_t interval_tree<T>::visit_contains(value_type *node,
					const endpoint_type &x, F &f) {
    local_stack<uintptr_t, 40> stack;
    value_type *next;
    size_t count = 0;
    if (!node)
	return count;

 left:
    while ((next = node->rblinks_.c_[0].node()) && x < next->subtree_iend()) {
	stack.push(reinterpret_cast<uintptr_t>(node));
	node = next;
    }

 middle:
    if (node->contains(x)) {
	f(*node);
	++count;
    }

    if (!(x < node->ibegin()) && (node = node->rblinks_.c_[1].node()))
	goto left;
    else if (stack.empty())
	return count;
    else {
	node = reinterpret_cast<value_type *>(stack.top());
	stack.pop();
	goto middle;
    }
}

template <typename T> template <typename F>
inline size_t interval_tree<T>::visit_contains(const endpoint_type &x,
					       const F &f) {
    typename std::decay<F>::type realf(f);
    return visit_contains(t_.root(), x, realf);
}

template <typename T> template <typename F>
inline size_t interval_tree<T>::visit_contains(const endpoint_type &x,
				   	          F &f) {
    return visit_contains(t_.root(), x, f);
}

template <typename T> template <typename F>
inline size_t interval_tree<T>::visit_overlaps(const endpoint_type &x,
					          const F &f) {
    typename std::decay<F>::type realf(f);
    return visit_contains(t_.root(), x, realf);
}

template <typename T> template <typename F>
inline size_t interval_tree<T>::visit_overlaps(const endpoint_type &x,
					          F &f) {
    return visit_contains(t_.root(), x, f);
}

template <typename T> template <typename I, typename F>
size_t interval_tree<T>::visit_overlaps(value_type *node, const I &x, F &f) {
    local_stack<uintptr_t, 40> stack;
    value_type *next;
    size_t count = 0;
    if (!node)
	return count;

 left:
    while ((next = node->rblinks_.c_[0].node()) && x.ibegin() < next->subtree_iend()) {
	stack.push(reinterpret_cast<uintptr_t>(node));
	node = next;
    }

 middle:
    if (node->overlaps(x)) {
	f(*node);
	++count;
    }

    if (node->ibegin() < x.iend() && (node = node->rblinks_.c_[1].node()))
	goto left;
    else if (stack.empty())
	return count;
    else {
	node = reinterpret_cast<value_type *>(stack.top());
	stack.pop();
	goto middle;
    }
}

template <typename T> template <typename I, typename F>
inline size_t interval_tree<T>::visit_overlaps(const I &x, const F &f) {
    typename std::decay<F>::type realf(f);
    return visit_overlaps(t_.root(), x, realf);
}

template <typename T> template <typename I, typename F>
inline size_t interval_tree<T>::visit_overlaps(const I &x, F &f) {
    return visit_overlaps(t_.root(), x, f);
}

template <typename T> template <typename I, typename F>
size_t interval_tree<T>::visit_contains(value_type *node, const I &x, F &f) {
    local_stack<uintptr_t, 40> stack;
    value_type *next;
    size_t count = 0;
    if (!node)
	return count;

 left:
    while ((next = node->rblinks_.c_[0].node()) && x.ibegin() < next->subtree_iend()) {
	stack.push(reinterpret_cast<uintptr_t>(node));
	node = next;
    }

 middle:
    if (node->contains(x)) {
	f(*node);
	++count;
    }

    if (node->ibegin() < x.iend() && (node = node->rblinks_.c_[1].node()))
	goto left;
    else if (stack.empty())
	return count;
    else {
	node = reinterpret_cast<value_type *>(stack.top());
	stack.pop();
	goto middle;
    }
}

template <typename T> template <typename I, typename F>
inline size_t interval_tree<T>::visit_contains(const I &x, const F &f) {
    typename std::decay<F>::type realf(f);
    return visit_contains(t_.root(), x, realf);
}

template <typename T> template <typename I, typename F>
inline size_t interval_tree<T>::visit_contains(const I &x, F &f) {
    return visit_contains(t_.root(), x, f);
}

template <typename T>
std::ostream &operator<<(std::ostream &s, const interval_tree<T> &tree) {
    return s << tree.t_;
}

#endif
