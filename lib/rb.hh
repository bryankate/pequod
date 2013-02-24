#ifndef GSTORE_RB_HH
#define GSTORE_RB_HH 1
#include <inttypes.h>
#include <iostream>
#include <iomanip>
#include "compiler.hh"
#include "local_stack.hh"
// LLRB 2-3 tree a la Sedgewick
// Intrusive

template <typename T> class rbnode;
template <typename T, typename Compare, typename Reshape> class rbtree;
namespace rbpriv {
template <typename T, typename Compare, typename Reshape> class rbrep1;
}

template <typename T>
class rbnodeptr {
  public:
    typedef bool (rbnodeptr<T>::*unspecified_bool_type)() const;

    inline rbnodeptr();
    inline rbnodeptr(T* x, bool color);
    explicit inline rbnodeptr(uintptr_t ivalue);

    inline operator unspecified_bool_type() const;
    inline bool operator!() const;
    inline uintptr_t ivalue() const;

    inline T* node() const;
    inline rbnodeptr<T>& child(bool isright) const;
    inline T*& parent() const;

    inline bool red() const;
    inline rbnodeptr<T> change_color(bool color) const;
    inline rbnodeptr<T> reverse_color() const;

    template <typename F> inline rbnodeptr<T> rotate(bool isright, F &f) const;
    template <typename F> inline rbnodeptr<T> rotate_left(F &f) const;
    template <typename F> inline rbnodeptr<T> rotate_right(F &f) const;
    inline rbnodeptr<T> flip() const;

    template <typename F> inline rbnodeptr<T> move_red_left(F &f) const;
    template <typename F> inline rbnodeptr<T> move_red_right(F &f) const;
    template <typename F> inline rbnodeptr<T> fixup(F &f) const;

    size_t size() const;
    template <typename C>
    void check(T* parent, int this_black_height, int& black_height,
               T* root, C& compare) const;
    void output(std::ostream& s, int indent, T* highlight) const;

  private:
    uintptr_t x_;
};

template <typename T>
class rblinks {
  public:
    T* child(bool right) const {
	return c_[right].node();
    }
    T* p_;
    rbnodeptr<T> c_[2];
};

namespace rbpriv {
template <typename Compare>
struct rbcompare : private Compare {
    inline rbcompare(const Compare &compare);
    template <typename T>
    inline int compare(const rbnode<T> &a, const rbnode<T> &b) const;
    template <typename A, typename B>
    inline int compare(const A &a, const B &b) const;
    inline Compare& get_compare() const;
};

template <typename Reshape, typename T>
struct rbreshape : private Reshape {
    inline rbreshape(const Reshape &reshape);
    inline rbreshape<Reshape, T> &reshape();
    inline void operator()(T* n);
};

template <typename T>
struct rbrep0 {
    T* root_;
};

template <typename T, typename Compare, typename Reshape>
struct rbrep1 : public rbrep0<T>, public rbcompare<Compare>,
		public rbreshape<Reshape, T> {
    typedef rbcompare<Compare> rbcompare_type;
    typedef rbreshape<Reshape, T> rbreshape_type;
    inline rbrep1(const Compare &compare, const Reshape &reshape);
    rbnodeptr<T> insert_node(rbnodeptr<T> np, T* x, T* p);
};
} // namespace rbpriv

template <typename T>
class rbalgorithms {
  public:
    static inline T* next_node(T* n);
    static inline T* prev_node(T* n);
    static inline T* step_node(T* n, bool forward);
    static inline T* edge_node(T* n, bool forward);
};

template <typename T>
class rbiterator {
  public:
    inline rbiterator();
    inline rbiterator(T* n);

    template <typename TT>
    friend inline bool operator==(rbiterator<TT> a, rbiterator<TT> b);
    template <typename TT>
    friend inline bool operator!=(rbiterator<TT> a, rbiterator<TT> b);

    inline void operator++(int);
    inline void operator++();
    inline void operator--(int);
    inline void operator--();

    inline T& operator*() const;
    inline T* operator->() const;

  private:
    T* n_;
};

template <typename T, typename Compare = default_comparator<T>,
	  typename Reshape = do_nothing>
class rbtree {
  public:
    typedef T value_type;
    typedef Compare value_compare;
    typedef Reshape reshape_type;
    typedef T node_type;

    inline rbtree(const value_compare &compare = value_compare(),
		  const reshape_type &reshape = reshape_type());
    ~rbtree();

    inline node_type *root();

    inline size_t size() const;

    typedef rbiterator<T> iterator;
    inline iterator begin() const;
    inline iterator end() const;
    inline iterator lower_bound(const value_type& x) const;
    template <typename K, typename Comp>
    inline iterator lower_bound(const K& key, Comp compare) const;
    inline iterator upper_bound(const value_type& x) const;
    template <typename K, typename Comp>
    inline iterator upper_bound(const K& key, Comp compare) const;

    template <typename X> inline node_type *find(const X &x);
    template <typename X> inline const node_type *find(const X &x) const;

    inline void insert(node_type* n);

    inline void erase(node_type* x);
    inline void erase_and_dispose(node_type* x);
    template <typename Disposer>
    inline void erase_and_dispose(node_type* x, Disposer d);

    int check() const;
    template <typename TT, typename CC, typename RR>
    friend std::ostream &operator<<(std::ostream &s, const rbtree<TT, CC, RR> &tree);

  private:
    rbpriv::rbrep1<T, Compare, Reshape> r_;

    //template <typename X> rbnode<T> *find_or_insert_node(const X& ctor);
    rbnodeptr<T> unlink_min(rbnodeptr<T> np, T** min);
    rbnodeptr<T> delete_node(rbnodeptr<T> np, T* victim);
    T* delete_node(T* victim);

    template <typename K, typename Comp>
    inline iterator lower_bound(const K& key, Comp& compare, bool) const;
    template <typename K, typename Comp>
    inline iterator lower_bound(const K& key, Comp& compare, int) const;
    template <typename K, typename Comp>
    inline iterator upper_bound(const K& key, Comp& compare, bool) const;
    template <typename K, typename Comp>
    inline iterator upper_bound(const K& key, Comp& compare, int) const;
};

template <typename T>
inline rbnodeptr<T>::rbnodeptr()
    : x_(0) {
}

template <typename T>
inline rbnodeptr<T>::rbnodeptr(T* x, bool color)
    : x_(reinterpret_cast<uintptr_t>(x) | color) {
}

template <typename T>
inline rbnodeptr<T>::rbnodeptr(uintptr_t x)
    : x_(x) {
}

template <typename T>
inline rbnodeptr<T>::operator unspecified_bool_type() const {
    return x_ != 0 ? &rbnodeptr<T>::operator! : 0;
}

template <typename T>
inline bool rbnodeptr<T>::operator!() const {
    return !x_;
}

template <typename T>
inline uintptr_t rbnodeptr<T>::ivalue() const {
    return x_;
}

template <typename T>
inline T* rbnodeptr<T>::node() const {
    return reinterpret_cast<T*>(x_ & ~uintptr_t(1));
}

template <typename T>
inline rbnodeptr<T>& rbnodeptr<T>::child(bool isright) const {
    return node()->rblinks_.c_[isright];
}

template <typename T>
inline T*& rbnodeptr<T>::parent() const {
    return node()->rblinks_.p_;
}

template <typename T>
inline bool rbnodeptr<T>::red() const {
    return x_ & 1;
}

template <typename T>
inline rbnodeptr<T> rbnodeptr<T>::change_color(bool color) const {
    return rbnodeptr<T>(color ? x_ | 1 : x_ & ~uintptr_t(1));
}

template <typename T>
inline rbnodeptr<T> rbnodeptr<T>::reverse_color() const {
    return rbnodeptr<T>(x_ ^ 1);
}

template <typename T> template <typename F>
inline rbnodeptr<T> rbnodeptr<T>::rotate(bool isright, F &f) const {
    rbnodeptr<T> x = child(!isright);
    if ((child(!isright) = x.child(isright)))
        x.child(isright).parent() = node();
    f(node());
    bool old_color = red();
    x.child(isright) = change_color(true);
    x.parent() = parent();
    parent() = x.node();
    f(x.node());
    return x.change_color(old_color);
}

template <typename T> template <typename F>
inline rbnodeptr<T> rbnodeptr<T>::rotate_left(F &f) const {
    return rotate(false, f);
}

template <typename T> template <typename F>
inline rbnodeptr<T> rbnodeptr<T>::rotate_right(F &f) const {
    return rotate(true, f);
}

template <typename T>
inline rbnodeptr<T> rbnodeptr<T>::flip() const {
    child(false) = child(false).reverse_color();
    child(true) = child(true).reverse_color();
    return reverse_color();
}

template <typename T> template <typename F>
inline rbnodeptr<T> rbnodeptr<T>::move_red_left(F &f) const {
    rbnodeptr<T> x = flip();
    if (x.child(true).child(false).red()) {
	x.child(true) = x.child(true).rotate_right(f);
	x = x.rotate_left(f);
	return x.flip();
    } else
	return x;
}

template <typename T> template <typename F>
inline rbnodeptr<T> rbnodeptr<T>::move_red_right(F &f) const {
    rbnodeptr x = flip();
    if (x.child(false).child(false).red()) {
	x = x.rotate_right(f);
	return x.flip();
    } else
	return x;
}

template <typename T> template <typename F>
inline rbnodeptr<T> rbnodeptr<T>::fixup(F &f) const {
    rbnodeptr<T> x = *this;
    f(x.node());
    if (x.child(true).red())
	x = x.rotate_left(f);
    if (x.child(false).red() && x.child(false).child(false).red())
	x = x.rotate_right(f);
    if (x.child(false).red() && x.child(true).red())
	x = x.flip();
    return x;
}

namespace rbpriv {
template <typename Compare>
inline rbcompare<Compare>::rbcompare(const Compare &compare)
    : Compare(compare) {
}

template <typename Compare> template <typename T>
inline int rbcompare<Compare>::compare(const rbnode<T> &a,
                                       const rbnode<T> &b) const {
    int cmp = this->operator()(a, b);
    return cmp ? cmp : default_compare(&a, &b);
}

template <typename Compare> template <typename A, typename B>
inline int rbcompare<Compare>::compare(const A &a, const B &b) const {
    return this->operator()(a, b);
}

template <typename Reshape, typename T>
inline rbreshape<Reshape, T>::rbreshape(const Reshape& reshape)
    : Reshape(reshape) {
}

template <typename Reshape, typename T>
inline void rbreshape<Reshape, T>::operator()(T* n) {
    static_cast<Reshape &>(*this)(n);
}

template <typename Reshape, typename T>
inline rbreshape<Reshape, T> &rbreshape<Reshape, T>::reshape() {
    return *this;
}

template <typename T, typename C, typename R>
inline rbrep1<T, C, R>::rbrep1(const C &compare, const R &reshape)
    : rbcompare_type(compare), rbreshape_type(reshape) {
    this->root_ = 0;
}

template <typename Compare>
inline Compare& rbcompare<Compare>::get_compare() const {
    return *const_cast<Compare*>(static_cast<const Compare*>(this));
}

} // namespace rbpriv

template <typename T, typename C, typename R>
inline rbtree<T, C, R>::rbtree(const value_compare &compare,
			       const reshape_type &reshape)
    : r_(compare, reshape) {
}

template <typename T, typename C, typename R>
rbtree<T, C, R>::~rbtree() {
}

namespace rbpriv {
template <typename T, typename C, typename R>
rbnodeptr<T> rbrep1<T, C, R>::insert_node(rbnodeptr<T> np, T* x, T* p) {
    if (!np) {
        x->rblinks_.p_ = p;
	this->reshape()(x);
	return rbnodeptr<T>(x, true);
    }

    T* n = np.node();
    int cmp = this->compare(*x, *n);
    if (cmp == 0)
	cmp = (x < n ? -1 : 1);

    n->rblinks_.c_[cmp > 0] = insert_node(n->rblinks_.c_[cmp > 0], x, n);
    return np.fixup(this->reshape());
}
} // namespace rbpriv

#if 0
template <typename T, typename C, typename R>
template <typename X>
rbnode<T> *rbtree<T, C, R>::find_or_insert_node(const X &args) {
    local_stack<uintptr_t, 40> stack;

    rbnodeptr<T> np = rbnodeptr<T>(r_.root_, false);
    while (np) {
	rbnode<T>* n = np.node();
	int cmp = r_.compare(args, *n);
	if (cmp == 0)
	    return n;
	stack.push(np.ivalue() | (cmp > 0 ? 4 : 0));
	np = n->rblinks_.c_[cmp > 0];
    }

    rbnode<T> *result = r_.allocator().allocate(1);
    r_.allocator().construct(result, args);
    r_.reshape()(result);
    np = rbnodeptr<T>(result, true);

    while (stack.size()) {
	bool child = stack.top() & 4;
	rbnodeptr<T> pnp(stack.top() & ~uintptr_t(4));
	pnp.node()->rblinks_.c_[child] = np;
	np = pnp.fixup(r_.reshape());
	stack.pop();
    }

    r_.root_ = np.node();
    return result;
}
#endif

template <typename T, typename C, typename R>
rbnodeptr<T> rbtree<T, C, R>::unlink_min(rbnodeptr<T> np, T** min) {
    T* n = np.node();
    if (!n->rblinks_.c_[0]) {
	*min = n;
	return rbnodeptr<T>();
    }
    if (!n->rblinks_.c_[0].red() && !n->rblinks_.c_[0].child(false).red()) {
	np = np.move_red_left(r_.reshape());
	n = np.node();
    }
    n->rblinks_.c_[0] = unlink_min(n->rblinks_.c_[0], min);
    return np.fixup(r_.reshape());
}

template <typename T, typename C, typename R>
rbnodeptr<T> rbtree<T, C, R>::delete_node(rbnodeptr<T> np, T* victim) {
    // XXX will break tree if nothing is removed
    if (r_.compare(*victim, *np.node()) < 0) {
	if (!np.child(false).red() && !np.child(false).child(false).red())
	    np = np.move_red_left(r_.reshape());
	np.child(false) = delete_node(np.child(false), victim);
    } else {
	if (np.child(false).red())
	    np = np.rotate_right(r_.reshape());
        if (victim == np.node() && !np.child(true))
	    return rbnodeptr<T>();
	if (!np.child(true).red() && !np.child(true).child(false).red())
	    np = np.move_red_right(r_.reshape());
	if (victim == np.node()) {
	    T* min;
	    np.child(true) = unlink_min(np.child(true), &min);
	    min->rblinks_ = np.node()->rblinks_;
            for (int i = 0; i < 2; ++i)
                if (min->rblinks_.c_[i])
                    min->rblinks_.c_[i].parent() = min;
	    np = rbnodeptr<T>(min, np.red());
	} else
	    np.child(true) = delete_node(np.child(true), victim);
    }
    return np.fixup(r_.reshape());
}

template <typename T, typename C, typename R>
T* rbtree<T, C, R>::delete_node(T* victim) {
    // construct path to root
    local_stack<T*, (sizeof(size_t) << 2)> stk;
    for (T* n = victim; n; n = n->rblinks_.p_)
        stk.push(n);

    // work backwards
    int si = stk.size() - 1;
    rbnodeptr<T> np(stk[si], false), repl;
    size_t childtrack = 0, redtrack = 0;
    while (1) {
        bool direction;
        if (si && np.child(false).node() == stk[si-1]) {
            if (!np.child(false).red() && !np.child(false).child(false).red())
                np = np.move_red_left(r_.reshape());
            direction = false;
        } else {
            if (np.child(false).red())
                np = np.rotate_right(r_.reshape());
            if (victim == np.node() && !np.child(true)) {
                repl = rbnodeptr<T>();
                break;
            }
            if (!np.child(true).red() && !np.child(true).child(false).red())
                np = np.move_red_right(r_.reshape());
            if (victim == np.node()) {
                T* min;
                np.child(true) = unlink_min(np.child(true), &min);
                min->rblinks_ = np.node()->rblinks_;
                for (int i = 0; i < 2; ++i)
                    if (min->rblinks_.c_[i])
                        min->rblinks_.c_[i].parent() = min;
                repl = rbnodeptr<T>(min, np.red()).fixup(r_.reshape());
                break;
            }
            direction = true;
        }
        childtrack = (childtrack << 1) | direction;
        redtrack = (redtrack << 1) | np.red();
        np = np.child(direction);
        if (np.node() != stk[si])
            --si;
    }

    // now work up
    if (T* p = np.parent())
        do {
            p->rblinks_.c_[childtrack & 1] = repl;
            repl = rbnodeptr<T>(p, redtrack & 1);
            repl = repl.fixup(r_.reshape());
            childtrack >>= 1;
            redtrack >>= 1;
            p = repl.parent();
        } while (p);
    return repl.node();
}

template <typename T, typename C, typename R>
inline T* rbtree<T, C, R>::root() {
    return r_.root_;
}

template <typename T, typename C, typename R> template <typename X>
inline T* rbtree<T, C, R>::find(const X &x) {
    T* n = r_.root_;
    while (n) {
	int cmp = r_.compare(x, *n);
	if (cmp < 0)
	    n = n->rblinks_.c_[0].node();
	else if (cmp == 0)
	    break;
	else
	    n = n->rblinks_.c_[1].node();
    }
    return n;
}

template <typename T, typename C, typename R> template <typename X>
inline const T* rbtree<T, C, R>::find(const X &x) const {
    return const_cast<rbtree<T, C, R> *>(this)->find(x);
}

template <typename T, typename C, typename R>
inline void rbtree<T, C, R>::insert(T* node) {
    r_.root_ = r_.insert_node(rbnodeptr<T>(r_.root_, false), node, 0).node();
}

template <typename T, typename C, typename R>
inline void rbtree<T, C, R>::erase(T* node) {
    //r_.root_ = delete_node(rbnodeptr<T>(r_.root_, false), node).node();
    r_.root_ = delete_node(node);
}

template <typename T, typename C, typename R>
inline void rbtree<T, C, R>::erase_and_dispose(T* node) {
    //r_.root_ = delete_node(rbnodeptr<T>(r_.root_, false), node).node();
    r_.root_ = delete_node(node);
    delete node;
}

template <typename T, typename C, typename R> template <typename Disposer>
inline void rbtree<T, C, R>::erase_and_dispose(T* node, Disposer d) {
    //r_.root_ = delete_node(rbnodeptr<T>(r_.root_, false), node).node();
    r_.root_ = delete_node(node);
    d(node);
}

template <typename T>
void rbnodeptr<T>::output(std::ostream &s, int indent, T* highlight) const {
    if (node()->rblinks_.c_[0])
	node()->rblinks_.c_[0].output(s, indent + 2, highlight);
    s << std::setw(indent) << "" << (const T&) *node() << " @" << (void*) node() << (red() ? " red" : "");
    if (highlight && highlight == node())
        s << " ******  p@" << (void*) node()->rblinks_.p_ << "\n";
    else
        s << "\n";
    if (node()->rblinks_.c_[1])
	node()->rblinks_.c_[1].output(s, indent + 2, highlight);
}

template <typename T, typename C, typename R>
std::ostream &operator<<(std::ostream &s, const rbtree<T, C, R> &tree) {
    if (tree.r_.root_)
	rbnodeptr<T>(tree.r_.root_, false).output(s, 0, 0);
    else
	s << "<empty>\n";
    return s;
}

template <typename T>
size_t rbnodeptr<T>::size() const {
    return 1 + (child(false) ? child(false).size() : 0)
	+ (child(true) ? child(true).size() : 0);
}

template <typename T, typename C, typename R>
inline size_t rbtree<T, C, R>::size() const {
    return r_.root_ ? rbnodeptr<T>(r_.root_, false).size() : 0;
}

#define rbcheck_assert(x) do { if (!(x)) { rbnodeptr<T>(root, false).output(std::cerr, 0, node()); assert(x); } } while (0)

template <typename T> template <typename C>
void rbnodeptr<T>::check(T* parent, int this_black_height, int& black_height,
                         T* root, C& compare) const {
    rbcheck_assert(node()->rblinks_.p_ == parent);
    if (parent) {
        int cmp = compare(*node(), *parent);
        if (parent->rblinks_.c_[0].node() == node())
            rbcheck_assert(cmp < 0 || (cmp == 0 && node() < parent));
        else
            rbcheck_assert(cmp > 0 || (cmp == 0 && node() > parent));
    }
    if (red())
        rbcheck_assert(!child(false).red() && !child(true).red());
    else {
        rbcheck_assert(child(false).red() || !child(true).red());
        ++this_black_height;
    }
    for (int i = 0; i < 2; ++i)
        if (child(i))
            child(i).check(node(), this_black_height, black_height,
                           root, compare);
        else if (black_height == -1)
            black_height = this_black_height;
        else
            assert(black_height == this_black_height);
}

#undef rbcheck_assert

template <typename T, typename C, typename R>
int rbtree<T, C, R>::check() const {
    int black_height = -1;
    if (r_.root_)
        rbnodeptr<T>(r_.root_, false).check(0, 0, black_height,
                                            r_.root_, r_.get_compare());
    return black_height;
}

template <typename T>
inline T* rbalgorithms<T>::edge_node(T* n, bool forward) {
    while (n->rblinks_.c_[forward])
        n = n->rblinks_.c_[forward].node();
    return n;
}

template <typename T>
inline T* rbalgorithms<T>::step_node(T* n, bool forward) {
    if (n->rblinks_.c_[forward])
        n = edge_node(n->rblinks_.c_[forward].node(), !forward);
    else {
        T* prev;
        do {
            prev = n;
            n = n->rblinks_.p_;
        } while (n && n->rblinks_.c_[!forward].node() != prev);
    }
    return n;
}

template <typename T>
inline T* rbalgorithms<T>::next_node(T* n) {
    return step_node(n, true);
}

template <typename T>
inline T* rbalgorithms<T>::prev_node(T* n) {
    return step_node(n, false);
}

template <typename T>
inline rbiterator<T>::rbiterator() {
}

template <typename T>
inline rbiterator<T>::rbiterator(T* n)
    : n_(n) {
}

template <typename T>
inline void rbiterator<T>::operator++(int) {
    n_ = rbalgorithms<T>::next_node(n_);
}

template <typename T>
inline void rbiterator<T>::operator++() {
    n_ = rbalgorithms<T>::next_node(n_);
}

template <typename T>
inline void rbiterator<T>::operator--(int) {
    n_ = rbalgorithms<T>::prev_node(n_);
}

template <typename T>
inline void rbiterator<T>::operator--() {
    n_ = rbalgorithms<T>::prev_node(n_);
}

template <typename T>
inline T& rbiterator<T>::operator*() const {
    return *n_;
}

template <typename T>
inline T* rbiterator<T>::operator->() const {
    return n_;
}

template <typename T>
inline bool operator==(rbiterator<T> a, rbiterator<T> b) {
    return a.operator->() == b.operator->();
}

template <typename T>
inline bool operator!=(rbiterator<T> a, rbiterator<T> b) {
    return a.operator->() != b.operator->();
}

template <typename T, typename R, typename A>
rbiterator<T> rbtree<T, R, A>::begin() const {
    return rbiterator<T>(r_.root_ ? rbalgorithms<T>::edge_node(r_.root_, false) : 0);
}

template <typename T, typename R, typename A>
rbiterator<T> rbtree<T, R, A>::end() const {
    return rbiterator<T>(0);
}

template <typename T, typename R, typename A> template <typename K, typename Comp>
inline rbiterator<T> rbtree<T, R, A>::lower_bound(const K& key, Comp& compare, bool) const {
    T* n = r_.root_;
    T* bound = 0;
    while (n) {
        bool cmp = compare(*n, key);
        if (!cmp)
            bound = n;
        n = n->rblinks_.c_[cmp].node();
    }
    return bound;
}

template <typename T, typename R, typename A> template <typename K, typename Comp>
inline rbiterator<T> rbtree<T, R, A>::lower_bound(const K& key, Comp& compare, int) const {
    T* n = r_.root_;
    T* bound = 0;
    while (n) {
        int cmp = compare(*n, key);
        if (cmp >= 0)
            bound = n;
        n = n->rblinks_.c_[cmp < 0].node();
    }
    return bound;
}

template <typename T, typename R, typename A> template <typename K, typename Comp>
inline rbiterator<T> rbtree<T, R, A>::lower_bound(const K& key, Comp compare) const {
    return lower_bound(key, compare, (decltype(compare(*r_.root_, key))) 0);
}

template <typename T, typename R, typename A>
inline rbiterator<T> rbtree<T, R, A>::lower_bound(const value_type& x) const {
    return lower_bound(x, r_.get_compare(), 0);
}

template <typename T, typename R, typename A> template <typename K, typename Comp>
inline rbiterator<T> rbtree<T, R, A>::upper_bound(const K& key, Comp& compare, bool) const {
    T* n = r_.root_;
    T* bound = 0;
    while (n) {
        bool cmp = compare(key, *n);
        if (cmp)
            bound = n;
        n = n->rblinks_.c_[!cmp].node();
    }
    return bound;
}

template <typename T, typename R, typename A> template <typename K, typename Comp>
inline rbiterator<T> rbtree<T, R, A>::upper_bound(const K& key, Comp& compare, int) const {
    T* n = r_.root_;
    T* bound = 0;
    while (n) {
        int cmp = compare(key, *n);
        if (cmp < 0)
            bound = n;
        n = n->rblinks_.c_[cmp >= 0].node();
    }
    return bound;
}

template <typename T, typename R, typename A> template <typename K, typename Comp>
inline rbiterator<T> rbtree<T, R, A>::upper_bound(const K& key, Comp compare) const {
    return upper_bound(key, compare, (decltype(compare(*r_.root_, key))) 0);
}

template <typename T, typename R, typename A>
inline rbiterator<T> rbtree<T, R, A>::upper_bound(const value_type& x) const {
    return lower_bound(x, r_.get_compare(), 0);
}

#endif
