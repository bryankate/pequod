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
    inline void operator()(rbnode<T> *n) {
	n->set_subtree_iend(n->iend());
	for (int i = 0; i < 2; ++i)
	    if (rbnode<T> *x = n->child(i))
		if (n->subtree_iend() < x->subtree_iend())
		    n->set_subtree_iend(x->subtree_iend());
    }
};

template <typename T, typename A = std::allocator<rbnode<T> > >
class interval_tree {
  public:
    typedef rbnode<T> value_type;
    typedef typename T::endpoint_type endpoint_type;

    inline interval_tree();

    template <typename X> inline value_type *find(const X &i);
    template <typename X> inline const value_type *find(const X &i) const;

    inline value_type *insert(const T &x);
    inline value_type *insert(T &&x);
    template <typename... Args> inline value_type *insert(Args&&... args);
    template <typename I> inline value_type &operator[](const I &x);

    inline void erase(value_type *x);

    inline void clear();

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

    template <typename TT, typename AA> friend std::ostream &operator<<(std::ostream &s, const interval_tree<TT, AA> &x);

  private:
    rbtree<T, interval_comparator, interval_rb_reshaper, A> t_;

    template <typename F>
    static size_t visit_contains(value_type *node, const endpoint_type &x, F &f);
    template <typename I, typename F>
    static size_t visit_contains(value_type *node, const I &x, F &f);
    template <typename I, typename F>
    static size_t visit_overlaps(value_type *node, const I &x, F &f);
};

template <typename T, typename A>
inline interval_tree<T, A>::interval_tree() {
}

template <typename T, typename A> template <typename X>
inline rbnode<T> *interval_tree<T, A>::find(const X &i) {
    return t_.find(i);
}

template <typename T, typename A> template <typename X>
inline const rbnode<T> *interval_tree<T, A>::find(const X &i) const {
    return t_.find(i);
}

template <typename T, typename A>
inline rbnode<T> *interval_tree<T, A>::insert(const T &x) {
    return t_.insert(x);
}

template <typename T, typename A>
inline rbnode<T> *interval_tree<T, A>::insert(T &&x) {
    return t_.insert(std::move(x));
}

template <typename T, typename A> template <typename... Args>
inline rbnode<T> *interval_tree<T, A>::insert(Args&&... args) {
    return t_.insert(std::forward<Args>(args)...);
}

template <typename T, typename A> template <typename I>
inline rbnode<T> &interval_tree<T, A>::operator[](const I &x) {
    return t_[x];
}

template <typename T, typename A>
inline void interval_tree<T, A>::erase(rbnode<T> *x) {
    t_.erase(x);
}

template <typename T, typename A>
inline void interval_tree<T, A>::clear() {
    t_.clear();
}

template <typename T, typename A> template <typename F>
size_t interval_tree<T, A>::visit_contains(value_type *node,
				   	   const endpoint_type &x, F &f) {
    local_stack<uintptr_t, 40> stack;
    value_type *next;
    size_t count = 0;
    if (!node)
	return count;

 left:
    while ((next = node->child(0)) && x < next->subtree_iend()) {
	stack.push(reinterpret_cast<uintptr_t>(node));
	node = next;
    }

 middle:
    if (node->contains(x)) {
	f(*node);
	++count;
    }

    if (!(x < node->ibegin()) && (node = node->child(1)))
	goto left;
    else if (stack.empty())
	return count;
    else {
	node = reinterpret_cast<value_type *>(stack.top());
	stack.pop();
	goto middle;
    }
}

template <typename T, typename A> template <typename F>
inline size_t interval_tree<T, A>::visit_contains(const endpoint_type &x,
					       const F &f) {
    typename std::decay<F>::type realf(f);
    return visit_contains(t_.root(), x, realf);
}

template <typename T, typename A> template <typename F>
inline size_t interval_tree<T, A>::visit_contains(const endpoint_type &x,
				   	          F &f) {
    return visit_contains(t_.root(), x, f);
}

template <typename T, typename A> template <typename F>
inline size_t interval_tree<T, A>::visit_overlaps(const endpoint_type &x,
					          const F &f) {
    typename std::decay<F>::type realf(f);
    return visit_contains(t_.root(), x, realf);
}

template <typename T, typename A> template <typename F>
inline size_t interval_tree<T, A>::visit_overlaps(const endpoint_type &x,
					          F &f) {
    return visit_contains(t_.root(), x, f);
}

template <typename T, typename A> template <typename I, typename F>
size_t interval_tree<T, A>::visit_overlaps(value_type *node, const I &x, F &f) {
    local_stack<uintptr_t, 40> stack;
    value_type *next;
    size_t count = 0;
    if (!node)
	return count;

 left:
    while ((next = node->child(0)) && x.ibegin() < next->subtree_iend()) {
	stack.push(reinterpret_cast<uintptr_t>(node));
	node = next;
    }

 middle:
    if (node->overlaps(x)) {
	f(*node);
	++count;
    }

    if (node->ibegin() < x.iend() && (node = node->child(1)))
	goto left;
    else if (stack.empty())
	return count;
    else {
	node = reinterpret_cast<value_type *>(stack.top());
	stack.pop();
	goto middle;
    }
}

template <typename T, typename A> template <typename I, typename F>
inline size_t interval_tree<T, A>::visit_overlaps(const I &x, const F &f) {
    typename std::decay<F>::type realf(f);
    return visit_overlaps(t_.root(), x, realf);
}

template <typename T, typename A> template <typename I, typename F>
inline size_t interval_tree<T, A>::visit_overlaps(const I &x, F &f) {
    return visit_overlaps(t_.root(), x, f);
}

template <typename T, typename A> template <typename I, typename F>
size_t interval_tree<T, A>::visit_contains(value_type *node, const I &x, F &f) {
    local_stack<uintptr_t, 40> stack;
    value_type *next;
    size_t count = 0;
    if (!node)
	return count;

 left:
    while ((next = node->child(0)) && x.ibegin() < next->subtree_iend()) {
	stack.push(reinterpret_cast<uintptr_t>(node));
	node = next;
    }

 middle:
    if (node->contains(x)) {
	f(*node);
	++count;
    }

    if (node->ibegin() < x.iend() && (node = node->child(1)))
	goto left;
    else if (stack.empty())
	return count;
    else {
	node = reinterpret_cast<value_type *>(stack.top());
	stack.pop();
	goto middle;
    }
}

template <typename T, typename A> template <typename I, typename F>
inline size_t interval_tree<T, A>::visit_contains(const I &x, const F &f) {
    typename std::decay<F>::type realf(f);
    return visit_contains(t_.root(), x, realf);
}

template <typename T, typename A> template <typename I, typename F>
inline size_t interval_tree<T, A>::visit_contains(const I &x, F &f) {
    return visit_contains(t_.root(), x, f);
}

template <typename T, typename A>
std::ostream &operator<<(std::ostream &s, const interval_tree<T, A> &tree) {
    return s << tree.t_;
}

#endif
