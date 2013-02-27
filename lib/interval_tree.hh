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
    inline bool operator()(T* n) {
	n->set_subtree_iend(n->iend());
	for (int i = 0; i < 2; ++i)
	    if (T* x = n->rblinks_.c_[i].node())
		if (n->subtree_iend() < x->subtree_iend())
		    n->set_subtree_iend(x->subtree_iend());
        return true;
    }
};

template <typename I> struct interval_interval_contains_predicate;
template <typename I> struct interval_interval_overlaps_predicate;
template <typename X> struct interval_endpoint_contains_predicate;
template <typename T, typename P> class interval_contains_iterator;

template <typename T>
class interval_tree {
  public:
    typedef T value_type;
    typedef typename T::endpoint_type endpoint_type;

    inline interval_tree();

    inline size_t size() const;

    template <typename X> inline value_type* find(const X &i);
    template <typename X> inline const value_type* find(const X &i) const;

    inline void insert(value_type* x);

    inline void erase(value_type* x);
    inline void erase_and_dispose(value_type* x);
    template <typename Dispose> inline void erase_and_dispose(value_type* x, Dispose d);

    typedef rbiterator<T> iterator;
    inline iterator begin() const;
    inline iterator end() const;

    template <typename I>
    inline interval_contains_iterator<T, interval_interval_contains_predicate<I> >
      begin_contains(const I& x);
    template <typename I>
    inline interval_contains_iterator<T, interval_interval_contains_predicate<I> >
      end_contains(const I& x);
    inline interval_contains_iterator<T, interval_endpoint_contains_predicate<endpoint_type> >
      begin_contains(const endpoint_type& x);
    template <typename I>
    inline interval_contains_iterator<T, interval_endpoint_contains_predicate<endpoint_type> >
      end_contains(const endpoint_type& x);
    template <typename I>
    inline interval_contains_iterator<T, interval_interval_overlaps_predicate<I> >
      begin_overlaps(const I& x);
    template <typename I>
    inline interval_contains_iterator<T, interval_interval_overlaps_predicate<I> >
      end_overlaps(const I& x);

    template <typename TT> friend std::ostream &operator<<(std::ostream &s, const interval_tree<TT> &x);

  private:
    rbtree<T, interval_comparator, interval_rb_reshaper> t_;
};

template <typename T>
inline interval_tree<T>::interval_tree() {
}

template <typename T>
inline size_t interval_tree<T>::size() const {
    return t_.size();
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

#if INTERVAL_TREE_DEBUG
# define IDEBUG(...) std::cerr << __VA_ARGS__
#else
# define IDEBUG(...)
#endif

template <typename I>
struct interval_interval_contains_predicate {
    const I& x_;
    interval_interval_contains_predicate(const I& x)
        : x_(x) {
    }
    template <typename T> bool check(T* node) {
	IDEBUG("check_contains[" << x_ << "] [" << node->ibegin() << ", " << node->iend() << ")\n");
        // XXX should allow selection
        return interval<typename T::endpoint_type>::contains(node->ibegin(), node->iend(), x_.ibegin(), x_.iend());
    }
    template <typename T> bool visit_subtree(T* node) {
	IDEBUG("visit_subtree[" << x_.ibegin() << "] " << node->subtree_iend() << "\n");
        return x_.ibegin() < node->subtree_iend();
    }
    template <typename T> bool visit_right(T* node) {
	IDEBUG("visit_right[" << x_.iend() << "] " << node->ibegin() << "\n");
        return node->ibegin() < x_.iend();
    }
};

template <typename I>
struct interval_interval_overlaps_predicate {
    const I& x_;
    interval_interval_overlaps_predicate(const I& x)
        : x_(x) {
    }
    template <typename T> bool check(T* node) {
	IDEBUG("check_overlaps[" << x_ << "] [" << node->ibegin() << ", " << node->iend() << ")\n");
        // XXX should allow selection
        return interval<typename T::endpoint_type>::overlaps(node->ibegin(), node->iend(), x_.ibegin(), x_.iend());
    }
    template <typename T> bool visit_subtree(T* node) {
	IDEBUG("visit_subtree[" << x_.ibegin() << "] " << node->subtree_iend() << "\n");
        return x_.ibegin() < node->subtree_iend();
    }
    template <typename T> bool visit_right(T* node) {
	IDEBUG("visit_right[" << x_.iend() << "] " << node->ibegin() << "\n");
        return node->ibegin() < x_.iend();
    }
};

template <typename X>
struct interval_endpoint_contains_predicate {
    const X& x_;
    interval_endpoint_contains_predicate(const X& x)
        : x_(x) {
    }
    template <typename T> bool check(T* node) {
	IDEBUG("check_contains[" << x_ << "] [" << node->ibegin() << ", " << node->iend() << ")\n");
        // XXX should allow selection
        return interval<typename T::endpoint_type>::contains(node->ibegin(), node->iend(), x_);
    }
    template <typename T> bool visit_subtree(T* node) {
	IDEBUG("visit_subtree[" << x_ << "] " << node->subtree_iend() << "\n");
        return x_ < node->subtree_iend();
    }
    template <typename T> bool visit_right(T* node) {
	IDEBUG("visit_right[" << x_ << "] " << node->ibegin() << "\n");
        return !(x_ < node->ibegin());
    }
};

#undef IDEBUG

template <typename T, typename P>
class interval_contains_iterator {
  public:
    interval_contains_iterator() {
    }
    interval_contains_iterator(T* root, const P& predicate)
        : node_(root), predicate_(predicate) {
        if (node_)
            advance(true);
    }

    inline bool operator==(const interval_contains_iterator<T, P>& x) {
        return node_ == x.node_;
    }
    template <typename X> inline bool operator==(const X& x) {
        return node_ == x.operator->();
    }
    inline bool operator!=(const interval_contains_iterator<T, P>& x) {
        return node_ != x.node_;
    }
    template <typename X> inline bool operator!=(const X& x) {
        return node_ != x.operator->();
    }

    void operator++() {
        advance(false);
    }
    void operator++(int) {
        advance(false);
    }

    T& operator*() const {
        return *node_;
    }
    T* operator->() const {
        return node_;
    }
  private:
    T* node_;
    P predicate_;

    void advance(bool first);
};

template <typename T, typename P>
void interval_contains_iterator<T, P>::advance(bool first) {
    T* next;
    if (first)
        goto first;
    do {
        if ((next = node_->rblinks_.c_[1].node())) {
            node_ = next;
        first:
            while ((next = node_->rblinks_.c_[0].node())
		   && predicate_.visit_subtree(next))
                node_ = next;
        } else {
            do {
                next = node_;
                if (!(node_ = node_->rblinks_.p_))
                    return;
            } while (node_->rblinks_.c_[1].node() == next);
	    // Since the interval tree is arranged by increasing ibegin(),
	    // the first time we encounter !visit_right(), we are done.
	    if (!predicate_.visit_right(node_)) {
		node_ = 0;
		return;
	    }
        }
    } while (!predicate_.check(node_));
}

template <typename T>
inline typename interval_tree<T>::iterator interval_tree<T>::begin() const {
    return t_.begin();
}

template <typename T>
inline typename interval_tree<T>::iterator interval_tree<T>::end() const {
    return t_.end();
}

template <typename T> template <typename X>
interval_contains_iterator<T, interval_interval_contains_predicate<X> > interval_tree<T>::begin_contains(const X& x) {
    return interval_contains_iterator<T, interval_interval_contains_predicate<X> >(t_.root(), x);
}

template <typename T> template <typename X>
interval_contains_iterator<T, interval_interval_contains_predicate<X> > interval_tree<T>::end_contains(const X& x) {
    return interval_contains_iterator<T, interval_interval_contains_predicate<X> >(0, x);
}

template <typename T>
interval_contains_iterator<T, interval_endpoint_contains_predicate<typename T::endpoint_type> > interval_tree<T>::begin_contains(const endpoint_type& x) {
    return interval_contains_iterator<T, interval_endpoint_contains_predicate<typename T::endpoint_type> >(t_.root(), x);
}

template <typename T> template <typename X>
interval_contains_iterator<T, interval_endpoint_contains_predicate<typename T::endpoint_type> > interval_tree<T>::end_contains(const endpoint_type& x) {
    return interval_contains_iterator<T, interval_endpoint_contains_predicate<typename T::endpoint_type> >(0, x);
}

template <typename T> template <typename X>
interval_contains_iterator<T, interval_interval_overlaps_predicate<X> > interval_tree<T>::begin_overlaps(const X& x) {
    return interval_contains_iterator<T, interval_interval_overlaps_predicate<X> >(t_.root(), x);
}

template <typename T> template <typename X>
interval_contains_iterator<T, interval_interval_overlaps_predicate<X> > interval_tree<T>::end_overlaps(const X& x) {
    return interval_contains_iterator<T, interval_interval_overlaps_predicate<X> >(0, x);
}

template <typename T>
std::ostream &operator<<(std::ostream &s, const interval_tree<T> &tree) {
    return s << tree.t_;
}

#endif
