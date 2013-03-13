#ifndef GSTORE_RB_HH
#define GSTORE_RB_HH 1
#include <inttypes.h>
#include <iostream>
#include <iomanip>
#include "compiler.hh"
#include "local_stack.hh"
#ifndef rbaccount
# define rbaccount(x)
#endif
// RB tree
// Intrusive

template <typename T, typename Compare, typename Reshape> class rbtree;
namespace rbpriv {
template <typename T, typename Compare, typename Reshape> class rbrep1;
}

template <typename T>
class rbnodeptr {
  public:
    typedef bool (rbnodeptr<T>::*unspecified_bool_type)() const;

    rbnodeptr() = default;
    inline rbnodeptr(T* x, bool color);
    explicit inline rbnodeptr(uintptr_t ivalue);

    inline operator unspecified_bool_type() const;
    inline bool operator!() const;
    inline uintptr_t ivalue() const;

    inline T* node() const;
    inline rbnodeptr<T>& child(bool isright) const;
    inline bool find_child(T* node) const;
    inline rbnodeptr<T> set_child(bool isright, rbnodeptr<T> x, T*& root) const;
    inline T*& parent() const;
    inline rbnodeptr<T> black_parent() const;

    inline bool red() const;
    inline rbnodeptr<T> change_color(bool color) const;
    inline rbnodeptr<T> reverse_color() const;

    template <typename F> inline rbnodeptr<T> rotate(bool isright, F &f) const;
    template <typename F> inline rbnodeptr<T> rotate_left(F &f) const;
    template <typename F> inline rbnodeptr<T> rotate_right(F &f) const;
    inline rbnodeptr<T> flip() const;

    size_t size() const;
    template <typename C>
    void check(T* parent, int this_black_height, int& black_height,
               T* root, T* begin, C& compare) const;
    void output(std::ostream& s, int indent, T* highlight) const;

  private:
    uintptr_t x_;
};

template <typename T>
class rblinks {
  public:
    T* p_;
    rbnodeptr<T> c_[2];
};

namespace rbpriv {
template <typename Compare>
struct rbcompare : private Compare {
    inline rbcompare(const Compare &compare);
    template <typename T>
    inline int node_compare(const T& a, const T& b) const;
    template <typename A, typename B>
    inline int compare(const A &a, const B &b) const;
    inline Compare& get_compare() const;
};

template <typename Reshape, typename T>
struct rbreshape : private Reshape {
    inline rbreshape(const Reshape &reshape);
    inline rbreshape<Reshape, T>& reshape();
    inline bool operator()(T* n);
};

template <typename T>
struct rbrep0 {
    T* root_;
    T* begin_;
};

template <typename T, typename Compare, typename Reshape>
struct rbrep1 : public rbrep0<T>, public rbcompare<Compare>,
		public rbreshape<Reshape, T> {
    typedef rbcompare<Compare> rbcompare_type;
    typedef rbreshape<Reshape, T> rbreshape_type;
    inline rbrep1(const Compare &compare, const Reshape &reshape);
    void insert_node(T* x);
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
	  typename Reshape = return_false>
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

    inline bool empty() const;
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

    inline node_type* unlink_leftmost_without_rebalance();

    int check() const;
    template <typename TT, typename CC, typename RR>
    friend std::ostream &operator<<(std::ostream &s, const rbtree<TT, CC, RR> &tree);

  private:
    rbpriv::rbrep1<T, Compare, Reshape> r_;

    void delete_node(T* victim);
    void delete_node_fixup(rbnodeptr<T> p, bool child);

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
inline bool rbnodeptr<T>::find_child(T* n) const {
    return x_ && node()->rblinks_.c_[1].node() == n;
}

template <typename T>
inline rbnodeptr<T> rbnodeptr<T>::set_child(bool isright, rbnodeptr<T> x, T*& root) const {
    if (x_)
	child(isright) = x;
    else
	root = x.node();
    return x;
}

template <typename T>
inline T*& rbnodeptr<T>::parent() const {
    return node()->rblinks_.p_;
}

template <typename T>
inline rbnodeptr<T> rbnodeptr<T>::black_parent() const {
    return rbnodeptr<T>(node()->rblinks_.p_, false);
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
    rbaccount(rotation);
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
    rbaccount(flip);
    child(false) = child(false).reverse_color();
    child(true) = child(true).reverse_color();
    return reverse_color();
}

namespace rbpriv {
template <typename Compare>
inline rbcompare<Compare>::rbcompare(const Compare& compare)
    : Compare(compare) {
}

template <typename Compare> template <typename T>
inline int rbcompare<Compare>::node_compare(const T& a, const T& b) const {
    int cmp = this->operator()(a, b);
    return cmp ? cmp : default_compare(&a, &b);
}

template <typename Compare> template <typename A, typename B>
inline int rbcompare<Compare>::compare(const A& a, const B& b) const {
    return this->operator()(a, b);
}

template <typename Reshape, typename T>
inline rbreshape<Reshape, T>::rbreshape(const Reshape& reshape)
    : Reshape(reshape) {
}

template <typename Reshape, typename T>
inline bool rbreshape<Reshape, T>::operator()(T* n) {
    return static_cast<Reshape &>(*this)(n);
}

template <typename Reshape, typename T>
inline rbreshape<Reshape, T> &rbreshape<Reshape, T>::reshape() {
    return *this;
}

template <typename T, typename C, typename R>
inline rbrep1<T, C, R>::rbrep1(const C &compare, const R &reshape)
    : rbcompare_type(compare), rbreshape_type(reshape) {
    this->root_ = this->begin_ = 0;
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
void rbrep1<T, C, R>::insert_node(T* x) {
    // find insertion point
    rbnodeptr<T> p(this->root_, false);
    bool child = false;
    while (p && (child = this->node_compare(*x, *p.node()) > 0,
                 p.child(child)))
        p = p.child(child);

    // link in new node; it's red
    x->rblinks_.p_ = p.node();
    x->rblinks_.c_[0] = x->rblinks_.c_[1] = rbnodeptr<T>(0, false);
    rbnodeptr<T> z(x, true);

    // maybe set begin
    if (!this->begin_ || (p.node() == this->begin_ && child == false))
        this->begin_ = x;

    // reshape
    if (this->reshape()(x)) {
        if (p)
            p.child(child) = z;
        do {
            x = x->rblinks_.p_;
        } while (x && this->reshape()(x));
    }

    // flip up the tree
    // invariant: z.red()
    rbnodeptr<T> gp;
    while (p.red() && (gp = p.black_parent(),
                       gp.child(0).red() && gp.child(1).red())) {
        p.child(child) = z;
        z = gp.flip();
        p = z.black_parent();
        child = p.find_child(z.node());
        // get correct color for `p`
        if (p && (gp = p.black_parent()))
            p = gp.child(gp.find_child(p.node()));
    }

    // maybe one last rotation (pair)
    // invariant: z.red()
    if (p.red()) {
        p.child(child) = z;
        gp = p.black_parent();
        bool gpchild = gp.find_child(p.node());
        if (gpchild != child)
            gp.child(gpchild) = p.rotate(gpchild, this->reshape());
        z = gp.rotate(!gpchild, this->reshape());
        p = z.black_parent();
        child = p.find_child(gp.node());
    }

    p.set_child(child, z, this->root_);
}
} // namespace rbpriv

template <typename T, typename C, typename R>
void rbtree<T, C, R>::delete_node(T* victim_node) {
    using std::swap;
    // find the node's color
    rbnodeptr<T> victim(victim_node, false);
    rbnodeptr<T> p = victim.black_parent();
    bool child = p.find_child(victim_node);

    // swap with successor if necessary
    rbnodeptr<T> succ;
    if (victim.child(0) && victim.child(1)) {
        succ = victim.child(true);
        bool schild = true;
        while (succ.child(false))
            schild = false, succ = succ.child(false);
        if (p)
            p.child(child) = succ.change_color(p.child(child).red());
        else
            r_.root_ = succ.node();
        swap(succ.node()->rblinks_, victim.node()->rblinks_);
        if (schild)
            succ.child(schild) = victim.change_color(succ.child(schild).red());
        succ.child(0).parent() = succ.child(1).parent() = succ.node();
        p = victim.black_parent();
        child = schild;
    } else
        succ = rbnodeptr<T>(0, false);

    // splice out victim
    bool active = !victim.child(false);
    rbnodeptr<T> x = victim.child(active);
    bool color = p && p.child(child).red();
    p.set_child(child, x, r_.root_);
    if (x)
        x.parent() = p.node();

    // maybe set begin_
    if (victim.node() == this->r_.begin_) {
        rbnodeptr<T> b = (x ? x : p);
        while (b && b.child(false))
            b = b.child(false);
        this->r_.begin_ = b.node();
    }

    // reshaping
    while (x && r_.reshape()(x.node())) {
        if (x.node() == succ.node())
            succ = rbnodeptr<T>(0, false);
        x = x.black_parent();
    }
    while (succ && r_.reshape()(succ.node()))
        succ = succ.black_parent();

    if (!color)
        delete_node_fixup(p, child);
}

template <typename T, typename C, typename R>
void rbtree<T, C, R>::delete_node_fixup(rbnodeptr<T> p, bool child) {
    while (p && !p.child(0).red() && !p.child(1).red()
           && !p.child(!child).child(0).red()
           && !p.child(!child).child(1).red()) {
        p.child(!child) = p.child(!child).change_color(true);
        T* node = p.node();
        p = p.black_parent();
        child = p.find_child(node);
    }

    if (p && !p.child(child).red()) {
        rbnodeptr<T> gp = p.black_parent();
        bool gpchild = gp.find_child(p.node());

        if (p.child(!child).red()) {
            // invariant: p is black (b/c one of its children is red)
            gp.set_child(gpchild, p.rotate(child, r_.reshape()), r_.root_);
            gp = p.black_parent(); // p is now further down the tree
            gpchild = child;       // (since we rotated in that direction)
        }

        rbnodeptr<T> w = p.child(!child);
        if (!w.child(0).red() && !w.child(1).red()) {
            p.child(!child) = w.change_color(true);
            p = p.change_color(false);
        } else {
            if (!w.child(!child).red())
                p.child(!child) = w.rotate(!child, r_.reshape());
            bool gpchild = gp.find_child(p.node());
            if (gp)
                p = gp.child(gpchild); // fetch correct color for `p`
            p = p.rotate(child, r_.reshape());
            p.child(0) = p.child(0).change_color(false);
            p.child(1) = p.child(1).change_color(false);
        }
        gp.set_child(gpchild, p, r_.root_);
    } else if (p)
        p.child(child) = p.child(child).change_color(false);
}

template <typename T, typename C, typename R>
inline T* rbtree<T, C, R>::root() {
    return r_.root_;
}

template <typename T, typename C, typename R> template <typename X>
inline T* rbtree<T, C, R>::find(const X& x) {
    T* n = r_.root_;
    while (n) {
	int cmp = r_.compare(x, *n);
	if (cmp == 0)
	    break;
	n = n->rblinks_.c_[cmp > 0].node();
    }
    return n;
}

template <typename T, typename C, typename R> template <typename X>
inline const T* rbtree<T, C, R>::find(const X& x) const {
    return const_cast<rbtree<T, C, R> *>(this)->find(x);
}

template <typename T, typename C, typename R>
inline void rbtree<T, C, R>::insert(T* node) {
    rbaccount(insert);
    r_.insert_node(node);
}

template <typename T, typename C, typename R>
inline void rbtree<T, C, R>::erase(T* node) {
    rbaccount(erase);
    delete_node(node);
}

template <typename T, typename C, typename R>
inline void rbtree<T, C, R>::erase_and_dispose(T* node) {
    rbaccount(erase);
    delete_node(node);
    delete node;
}

template <typename T, typename C, typename R> template <typename Disposer>
inline void rbtree<T, C, R>::erase_and_dispose(T* node, Disposer d) {
    rbaccount(erase);
    delete_node(node);
    d(node);
}

template <typename T, typename C, typename R>
inline T* rbtree<T, C, R>::unlink_leftmost_without_rebalance() {
    T* node = r_.begin_;
    if (node) {
        rbnodeptr<T> n(node, false);
        if (n.child(true)) {
            n.child(true).parent() = n.parent();
            r_.begin_ = rbalgorithms<T>::edge_node(n.child(true).node(), false);
        } else
            r_.begin_ = n.parent();
    } else
        r_.root_ = 0;
    return node;
}

template <typename T>
void rbnodeptr<T>::output(std::ostream &s, int indent, T* highlight) const {
    if (!*this)
	s << "<empty>\n";
    else {
	if (child(0))
	    child(0).output(s, indent + 2, highlight);
	s << std::setw(indent) << "" << (const T&) *node() << " @" << (void*) node() << (red() ? " red" : "");
	if (highlight && highlight == node())
	    s << " ******  p@" << (void*) node()->rblinks_.p_ << "\n";
	else
	    s << "\n";
	if (child(1))
	    child(1).output(s, indent + 2, highlight);
    }
}

template <typename T, typename C, typename R>
std::ostream &operator<<(std::ostream &s, const rbtree<T, C, R> &tree) {
    rbnodeptr<T>(tree.r_.root_, false).output(s, 0, 0);
    return s;
}

template <typename T>
size_t rbnodeptr<T>::size() const {
    return 1 + (child(false) ? child(false).size() : 0)
	+ (child(true) ? child(true).size() : 0);
}

template <typename T, typename C, typename R>
inline bool rbtree<T, C, R>::empty() const {
    return !r_.root_;
}

template <typename T, typename C, typename R>
inline size_t rbtree<T, C, R>::size() const {
    return r_.root_ ? rbnodeptr<T>(r_.root_, false).size() : 0;
}

#define rbcheck_assert(x) do { if (!(x)) { rbnodeptr<T>(root, false).output(std::cerr, 0, node()); assert(x); } } while (0)

template <typename T> template <typename C>
void rbnodeptr<T>::check(T* parent, int this_black_height, int& black_height,
                         T* root, T* begin, C& compare) const {
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
        //rbcheck_assert(child(false).red() || !child(true).red()); /* LLRB */
        ++this_black_height;
    }
    if (begin && !child(0))
        assert(begin == node());
    for (int i = 0; i < 2; ++i)
        if (child(i))
            child(i).check(node(), this_black_height, black_height,
                           root, i ? 0 : begin, compare);
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
        rbnodeptr<T>(r_.root_, false).check(0, 0, black_height, r_.root_,
                                            r_.begin_, r_.get_compare());
    else
        assert(r_.begin_ == 0);
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
    return rbiterator<T>(r_.begin_);
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
