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

    void output(std::ostream& s, int indent) const;

  private:
    uintptr_t x_;
};

template <typename T>
class rblinks {
  public:
    T* child(bool right) const {
	return c_[right].node();
    }
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
    rbnodeptr<T> insert_node(rbnodeptr<T> np, T* x);
};
} // namespace rbpriv

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

    template <typename X> inline node_type *find(const X &x);
    template <typename X> inline const node_type *find(const X &x) const;

    inline void insert(node_type* n);

    inline void erase(node_type* x);
    inline void erase_and_dispose(node_type* x);
    template <typename Disposer>
    inline void erase_and_dispose(node_type* x, Disposer d);

    template <typename TT, typename CC, typename RR>
    friend std::ostream &operator<<(std::ostream &s, const rbtree<TT, CC, RR> &tree);

  private:
    rbpriv::rbrep1<T, Compare, Reshape> r_;

    //template <typename X> rbnode<T> *find_or_insert_node(const X& ctor);
    rbnodeptr<T> unlink_min(rbnodeptr<T> np, T** min);
    rbnodeptr<T> delete_node(rbnodeptr<T> np, T* victim);
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
    rbnodeptr<T> x = node()->rblinks_.c_[!isright];
    node()->rblinks_.c_[!isright] = x.node()->rblinks_.c_[isright];
    f(node());
    bool old_color = red();
    x.node()->rblinks_.c_[isright] = change_color(true);
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
    node()->rblinks_.c_[0] = node()->rblinks_.c_[0].reverse_color();
    node()->rblinks_.c_[1] = node()->rblinks_.c_[1].reverse_color();
    return reverse_color();
}

template <typename T> template <typename F>
inline rbnodeptr<T> rbnodeptr<T>::move_red_left(F &f) const {
    rbnodeptr<T> x = flip();
    if (x.node()->rblinks_.c_[1].node()->rblinks_.c_[0].red()) {
	x.node()->rblinks_.c_[1] = x.node()->rblinks_.c_[1].rotate_right(f);
	x = x.rotate_left(f);
	return x.flip();
    } else
	return x;
}

template <typename T> template <typename F>
inline rbnodeptr<T> rbnodeptr<T>::move_red_right(F &f) const {
    rbnodeptr x = flip();
    if (x.node()->rblinks_.child(0)->rblinks_.c_[0].red()) {
	x = x.rotate_right(f);
	return x.flip();
    } else
	return x;
}

template <typename T> template <typename F>
inline rbnodeptr<T> rbnodeptr<T>::fixup(F &f) const {
    rbnodeptr<T> x = *this;
    f(x.node());
    if (x.node()->rblinks_.c_[1].red())
	x = x.rotate_left(f);
    if (x.node()->rblinks_.c_[0].red() && x.node()->rblinks_.c_[0].node()->rblinks_.c_[0].red())
	x = x.rotate_right(f);
    if (x.node()->rblinks_.c_[0].red() && x.node()->rblinks_.c_[1].red())
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
rbnodeptr<T> rbrep1<T, C, R>::insert_node(rbnodeptr<T> np, T* x) {
    if (!np) {
	this->reshape()(x);
	return rbnodeptr<T>(x, true);
    }

    T* n = np.node();
    int cmp = this->compare(*x, *n);
    if (cmp == 0)
	cmp = (x < n ? -1 : 1);

    n->rblinks_.c_[cmp > 0] = insert_node(n->rblinks_.c_[cmp > 0], x);
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
    if (!n->rblinks_.c_[0].red() && !n->rblinks_.c_[0].node()->rblinks_.c_[0].red()) {
	np = np.move_red_left(r_.reshape());
	n = np.node();
    }
    n->rblinks_.c_[0] = unlink_min(n->rblinks_.c_[0], min);
    return np.fixup(r_.reshape());
}

template <typename T, typename C, typename R>
rbnodeptr<T> rbtree<T, C, R>::delete_node(rbnodeptr<T> np, T* victim) {
    // XXX will break tree if nothing is removed
    T* n = np.node();
    int cmp = r_.compare(*victim, *n);
    if (cmp < 0) {
	if (!n->rblinks_.c_[0].red() && !n->rblinks_.c_[0].node()->rblinks_.c_[0].red()) {
	    np = np.move_red_left(r_.reshape());
	    n = np.node();
	}
	n->rblinks_.c_[0] = delete_node(n->rblinks_.c_[0], victim);
    } else {
	if (n->rblinks_.c_[0].red()) {
	    np = np.rotate_right(r_.reshape());
	    n = np.node();
	    cmp = r_.compare(*victim, *n);
	}
	if (cmp == 0 && !n->rblinks_.c_[1])
	    return rbnodeptr<T>();
	if (!n->rblinks_.c_[1].red() && !n->rblinks_.c_[1].node()->rblinks_.c_[0].red()) {
	    np = np.move_red_right(r_.reshape());
	    if (np.node() != n) {
		n = np.node();
		cmp = r_.compare(*victim, *n);
	    }
	}
	if (cmp == 0) {
	    T* min;
	    n->rblinks_.c_[1] = unlink_min(n->rblinks_.c_[1], &min);
	    min->rblinks_.c_[0] = n->rblinks_.c_[0];
	    min->rblinks_.c_[1] = n->rblinks_.c_[1];
	    np = rbnodeptr<T>(min, np.red());
	} else
	    n->rblinks_.c_[1] = delete_node(n->rblinks_.c_[1], victim);
    }
    return np.fixup(r_.reshape());
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
    r_.root_ = r_.insert_node(rbnodeptr<T>(r_.root_, false), node).node();
}

template <typename T, typename C, typename R>
inline void rbtree<T, C, R>::erase(T* node) {
    r_.root_ = delete_node(rbnodeptr<T>(r_.root_, false), node).node();
}

template <typename T, typename C, typename R>
inline void rbtree<T, C, R>::erase_and_dispose(T* node) {
    r_.root_ = delete_node(rbnodeptr<T>(r_.root_, false), node).node();
    delete node;
}

template <typename T, typename C, typename R> template <typename Disposer>
inline void rbtree<T, C, R>::erase_and_dispose(T* node, Disposer d) {
    r_.root_ = delete_node(rbnodeptr<T>(r_.root_, false), node).node();
    d(node);
}

template <typename T>
void rbnodeptr<T>::output(std::ostream &s, int indent) const {
    if (node()->rblinks_.c_[0])
	node()->rblinks_.c_[0].output(s, indent + 2);
    s << std::setw(indent) << "" << (const T&) *node() << " @" << (void*) node() << (red() ? " red" : "") << std::endl;
    if (node()->rblinks_.c_[1])
	node()->rblinks_.c_[1].output(s, indent + 2);
}

template <typename T, typename C, typename R>
std::ostream &operator<<(std::ostream &s, const rbtree<T, C, R> &tree) {
    if (tree.r_.root_)
	rbnodeptr<T>(tree.r_.root_, false).output(s, 0);
    else
	s << "<empty>\n";
    return s;
}

#endif
