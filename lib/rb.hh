#ifndef GSTORE_RB_HH
#define GSTORE_RB_HH 1
#include <inttypes.h>
#include <iostream>
#include <iomanip>
#include <iterator>
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
    inline bool children_same_color() const;
    inline bool find_child(T* node) const;
    inline rbnodeptr<T>& load_color();
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
               T* root, T* begin, T* end, C& compare) const;
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
    T* limit_[2];
};

template <typename T, typename Compare, typename Reshape>
struct rbrep1 : public rbrep0<T>, public rbcompare<Compare>,
		public rbreshape<Reshape, T> {
    typedef rbcompare<Compare> rbcompare_type;
    typedef rbreshape<Reshape, T> rbreshape_type;
    inline rbrep1(const Compare &compare, const Reshape &reshape);
};

template <typename C, typename Ret> class rbcomparator;

template <typename C>
class rbcomparator<C, bool> {
  public:
    inline rbcomparator(C comp)
        : comp_(comp) {
    }
    template <typename A, typename B>
    inline bool less(const A& a, const B& b) {
        return comp_(a, b);
    }
    template <typename A, typename B>
    inline bool greater(const A& a, const B& b) {
        return comp_(b, a);
    }
    template <typename A, typename B>
    inline int compare(const A& a, const B& b) {
        return comp_(a, b) ? -1 : comp_(b, a);
    }
  private:
    C comp_;
};

template <typename C>
class rbcomparator<C, int> {
  public:
    inline rbcomparator(C comp)
        : comp_(comp) {
    }
    template <typename A, typename B>
    inline bool less(const A& a, const B& b) {
        return comp_(a, b) < 0;
    }
    template <typename A, typename B>
    inline bool greater(const A& a, const B& b) {
        return comp_(a, b) > 0;
    }
    template <typename A, typename B>
    inline int compare(const A& a, const B& b) {
        return comp_(a, b);
    }
  private:
    C comp_;
};

template <typename K, typename T, typename C>
inline auto make_compare(C comp) -> rbcomparator<C, decltype(comp(*(K*)nullptr, *(T*)nullptr))> {
    return rbcomparator<C, decltype(comp(*(K*)nullptr, *(T*)nullptr))>(comp);
}

} // namespace rbpriv

template <typename T>
class rbalgorithms {
  public:
    static inline T* next_node(T* n);
    static inline T* prev_node(T* n);
    static inline T* prev_node(T* n, T* last);
    static inline T* step_node(T* n, bool forward);
    static inline T* edge_node(T* n, bool forward);
};

template <typename T>
class rbconst_iterator : public std::iterator<std::forward_iterator_tag, T> {
  public:
    inline rbconst_iterator() = default;
    inline rbconst_iterator(const T* n);

    template <typename TT>
    friend inline bool operator==(rbconst_iterator<TT> a, rbconst_iterator<TT> b);
    template <typename TT>
    friend inline bool operator!=(rbconst_iterator<TT> a, rbconst_iterator<TT> b);

    inline void operator++(int);
    inline void operator++();
    inline void operator--(int);
    inline void operator--();

    inline const T& operator*() const;
    inline const T* operator->() const;

  protected:
    T* n_;
};

template <typename T>
class rbiterator : public rbconst_iterator<T> {
  public:
    inline rbiterator() = default;
    inline rbiterator(T* n);

    inline T& operator*() const;
    inline T* operator->() const;
};

template <typename T, typename Compare = default_comparator<T>,
	  typename Reshape = return_false>
class rbtree {
  public:
    typedef T* pointer;
    typedef const T* const_pointer;
    typedef T value_type;
    typedef value_type key_type;
    typedef T& reference;
    typedef const T& const_reference;
    typedef Compare value_compare;
    typedef Reshape reshape_type;
    typedef T node_type;

    inline rbtree(const value_compare &compare = value_compare(),
		  const reshape_type &reshape = reshape_type());
    ~rbtree();

    inline node_type *root();

    inline bool empty() const;
    inline size_t size() const;

    typedef rbconst_iterator<T> const_iterator;
    typedef rbiterator<T> iterator;
    inline const_iterator begin() const;
    inline iterator begin();
    inline const_iterator end() const;
    inline iterator end();
    inline const_iterator lower_bound(const_reference x) const;
    template <typename K, typename Comp>
    inline const_iterator lower_bound(const K& key, Comp compare) const;
    inline iterator lower_bound(const_reference x);
    template <typename K, typename Comp>
    inline iterator lower_bound(const K& key, Comp compare);
    inline const_iterator upper_bound(const_reference x) const;
    template <typename K, typename Comp>
    inline const_iterator upper_bound(const K& key, Comp compare) const;
    inline iterator upper_bound(const_reference x);
    template <typename K, typename Comp>
    inline iterator upper_bound(const K& key, Comp compare);

    inline const_iterator find(const_reference& x) const;
    inline iterator find(const_reference& x);
    template <typename K, typename Comp>
    inline const_iterator find(const K& key, Comp compare) const;
    template <typename K, typename Comp>
    inline iterator find(const K& key, Comp compare);

    inline const_iterator iterator_to(const_reference x) const;
    inline iterator iterator_to(reference x);

    inline void insert(reference n);

    typedef std::pair<rbnodeptr<T>, bool> insert_commit_data;
    template <typename K, typename Comp>
    inline std::pair<iterator, bool>
      insert_unique_check(const K& key, Comp compare, insert_commit_data& commit_data);
    template <typename K, typename Comp>
    inline std::pair<iterator, bool>
      insert_unique_check(const_iterator hint, const K& key, Comp compare, insert_commit_data& commit_data);
    inline iterator insert_unique_commit(reference x, const insert_commit_data& commit_data);

    inline iterator erase(iterator it);
    inline void erase(reference x);
    inline void erase_and_dispose(reference x);
    template <typename Disposer>
    inline void erase_and_dispose(reference x, Disposer d);

    inline node_type* unlink_leftmost_without_rebalance();

    int check() const;
    template <typename TT, typename CC, typename RR>
    friend std::ostream &operator<<(std::ostream &s, const rbtree<TT, CC, RR> &tree);

  private:
    rbpriv::rbrep1<T, Compare, Reshape> r_;

    void insert_commit(T* x, rbnodeptr<T> p, bool side);
    void delete_node(T* victim, T* successor_hint);
    void delete_node_fixup(rbnodeptr<T> p, bool side);

    template <typename K, typename Comp>
    inline std::pair<iterator, bool>
      insert_unique_check_impl(const K& key, Comp comp, insert_commit_data& commit_data);
    template <typename K, typename Comp>
    inline std::pair<iterator, bool>
      insert_unique_check_impl(const_iterator hint, const K& key, Comp comp, insert_commit_data& commit_data);
    template <typename K, typename Comp>
    inline T* find_any(const K& key, Comp comp) const;
    template <typename K, typename Comp>
    inline T* find_first(const K& key, Comp comp) const;
    template <typename K, typename Comp>
    inline T* lower_bound_impl(const K& key, Comp comp) const;
    template <typename K, typename Comp>
    inline T* upper_bound_impl(const K& key, Comp comp) const;
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
inline bool rbnodeptr<T>::children_same_color() const {
    return ((node()->rblinks_.c_[0].x_ ^ node()->rblinks_.c_[1].x_) & 1) == 0;
}

template <typename T>
inline bool rbnodeptr<T>::find_child(T* n) const {
    return x_ && node()->rblinks_.c_[1].node() == n;
}

template <typename T>
inline rbnodeptr<T>& rbnodeptr<T>::load_color() {
    assert(!red());
    rbnodeptr<T> p;
    if (x_ && (p = black_parent()) && p.child(p.find_child(node())).red())
        x_ |= 1;
    return *this;
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
inline rbnodeptr<T> rbnodeptr<T>::rotate(bool side, F &f) const {
    rbaccount(rotation);
    rbnodeptr<T> x = child(!side);
    if ((child(!side) = x.child(side)))
        x.child(side).parent() = node();
    f(node());
    bool old_color = red();
    x.child(side) = change_color(true);
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
    this->root_ = this->limit_[0] = this->limit_[1] = 0;
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

template <typename T, typename C, typename R> template <typename K, typename Comp>
auto rbtree<T, C, R>::insert_unique_check_impl(const K& key, Comp comp, insert_commit_data& commit_data)
    -> std::pair<iterator, bool> {
    rbnodeptr<T> p(r_.root_, false);
    int cmp = 0;
    while (p) {
        cmp = comp.compare(key, *p.node());
        if (cmp == 0)
            return std::pair<iterator, bool>(iterator(p.node()), false);
        if (!p.child(cmp > 0))
            break;
        p = p.child(cmp > 0);
    }
    commit_data.first = p;
    commit_data.second = cmp > 0;
    return std::pair<iterator, bool>(iterator(), true);
}

template <typename T, typename C, typename R> template <typename K, typename Comp>
auto rbtree<T, C, R>::insert_unique_check(const K& key, Comp comp,
                                          insert_commit_data& commit_data)
    -> std::pair<iterator, bool> {
    return insert_unique_check_impl(key, rbpriv::make_compare<K, T>(comp), commit_data);
}

template <typename T, typename C, typename R> template <typename K, typename Comp>
auto rbtree<T, C, R>::insert_unique_check_impl(const_iterator hint, const K& key, Comp comp,
                                               insert_commit_data& commit_data)
    -> std::pair<iterator, bool> {
    T* h = const_cast<T*>(hint.operator->());
    if (!h || comp.less(key, *h)) {
        T* prev = h;
        if (prev == r_.limit_[0]
            || (prev = rbalgorithms<T>::prev_node(prev, r_.limit_[1]),
                comp.greater(key, *prev))) {
            if ((commit_data.second = !h || h->rblinks_.c_[false]))
                h = prev;
            commit_data.first = rbnodeptr<T>(h, false).load_color();
            return std::pair<iterator, bool>(nullptr, true);
        }
    }
    return insert_unique_check_impl(key, comp, commit_data);
}

template <typename T, typename C, typename R> template <typename K, typename Comp>
auto rbtree<T, C, R>::insert_unique_check(const_iterator hint, const K& key, Comp comp,
                                          insert_commit_data& commit_data)
    -> std::pair<iterator, bool> {
    return insert_unique_check_impl(hint, key, rbpriv::make_compare<K, T>(comp), commit_data);
}

template <typename T, typename C, typename R>
void rbtree<T, C, R>::insert_commit(T* x, rbnodeptr<T> p, bool side) {
    // link in new node; it's red
    x->rblinks_.p_ = p.node();
    x->rblinks_.c_[0] = x->rblinks_.c_[1] = rbnodeptr<T>(0, false);

    // maybe set limits
    if (p) {
        p.child(side) = rbnodeptr<T>(x, true);
        if (p.node() == r_.limit_[side])
            r_.limit_[side] = x;
    } else
        r_.root_ = r_.limit_[0] = r_.limit_[1] = x;

    // reshape
    if (r_.reshape()(x))
        do {
            x = x->rblinks_.p_;
        } while (x && r_.reshape()(x));

    // flip up the tree
    // invariant: we are looking at the `side` of `p`
    while (p.red()) {
        rbnodeptr<T> gp = p.black_parent(), z;
        if (gp.child(0).red() && gp.child(1).red()) {
            z = gp.flip();
            p = gp.black_parent().load_color();
        } else {
            bool gpside = gp.find_child(p.node());
            if (gpside != side)
                gp.child(gpside) = p.rotate(gpside, r_.reshape());
            z = gp.rotate(!gpside, r_.reshape());
            p = z.black_parent();
        }
        side = p.find_child(gp.node());
        p.set_child(side, z, r_.root_);
    }
}

template <typename T, typename C, typename R>
inline auto rbtree<T, C, R>::insert_unique_commit(reference x, const insert_commit_data& commit_data)
    -> iterator {
    insert_commit(&x, commit_data.first, commit_data.second);
    return iterator(&x);
}

template <typename T, typename C, typename R>
void rbtree<T, C, R>::insert(reference x) {
    rbaccount(insert);

    // find insertion point
    rbnodeptr<T> p(r_.root_, false);
    bool side = false;
    while (p && (side = r_.node_compare(x, *p.node()) > 0,
                 p.child(side)))
        p = p.child(side);

    insert_commit(&x, p, side);
}

template <typename T, typename C, typename R>
void rbtree<T, C, R>::delete_node(T* victim_node, T* succ) {
    using std::swap;
    // find the node's color
    rbnodeptr<T> victim(victim_node, false);
    rbnodeptr<T> p = victim.black_parent();
    bool side = p.find_child(victim_node);

    // swap with successor if necessary
    if (victim.child(0) && victim.child(1)) {
        if (!succ)
            for (succ = victim.child(true).node();
                 succ->rblinks_.c_[0];
                 succ = succ->rblinks_.c_[0].node())
                /* nada */;
        rbnodeptr<T> succ_p = rbnodeptr<T>(succ->rblinks_.p_, false);
        bool sside = succ == succ_p.child(true).node();
        if (p)
            p.child(side) = rbnodeptr<T>(succ, p.child(side).red());
        else
            r_.root_ = succ;
        swap(succ->rblinks_, victim.node()->rblinks_);
        if (sside)
            succ->rblinks_.c_[sside] = victim.change_color(succ->rblinks_.c_[sside].red());
        succ->rblinks_.c_[0].parent() = succ->rblinks_.c_[1].parent() = succ;
        p = victim.black_parent();
        side = sside;
    } else
        succ = nullptr;

    // splice out victim
    bool active = !victim.child(false);
    rbnodeptr<T> x = victim.child(active);
    bool color = p && p.child(side).red();
    p.set_child(side, x, r_.root_);
    if (x)
        x.parent() = p.node();

    // maybe set limits
    for (int i = 0; i != 2; ++i)
        if (victim.node() == this->r_.limit_[i]) {
            rbnodeptr<T> b = (x ? x : p);
            while (b && b.child(i))
                b = b.child(i);
            this->r_.limit_[i] = b.node();
        }

    // reshaping
    while (x && r_.reshape()(x.node())) {
        if (x.node() == succ)
            succ = nullptr;
        x = x.black_parent();
    }
    while (succ && r_.reshape()(succ))
        succ = succ->rblinks_.p_;

    if (!color)
        delete_node_fixup(p, side);
}

template <typename T, typename C, typename R>
void rbtree<T, C, R>::delete_node_fixup(rbnodeptr<T> p, bool side) {
    while (p && !p.child(0).red() && !p.child(1).red()
           && !p.child(!side).child(0).red()
           && !p.child(!side).child(1).red()) {
        p.child(!side) = p.child(!side).change_color(true);
        T* node = p.node();
        p = p.black_parent();
        side = p.find_child(node);
    }

    if (p && !p.child(side).red()) {
        rbnodeptr<T> gp = p.black_parent();
        bool gpside = gp.find_child(p.node());

        if (p.child(!side).red()) {
            // invariant: p is black (b/c one of its children is red)
            gp.set_child(gpside, p.rotate(side, r_.reshape()), r_.root_);
            gp = p.black_parent(); // p is now further down the tree
            gpside = side;         // (since we rotated in that direction)
        }

        rbnodeptr<T> w = p.child(!side);
        if (!w.child(0).red() && !w.child(1).red()) {
            p.child(!side) = w.change_color(true);
            p = p.change_color(false);
        } else {
            if (!w.child(!side).red())
                p.child(!side) = w.rotate(!side, r_.reshape());
            bool gpside = gp.find_child(p.node());
            if (gp)
                p = gp.child(gpside); // fetch correct color for `p`
            p = p.rotate(side, r_.reshape());
            p.child(0) = p.child(0).change_color(false);
            p.child(1) = p.child(1).change_color(false);
        }
        gp.set_child(gpside, p, r_.root_);
    } else if (p)
        p.child(side) = p.child(side).change_color(false);
}

template <typename T, typename C, typename R>
inline T* rbtree<T, C, R>::root() {
    return r_.root_;
}

template <typename T, typename C, typename R> template <typename K, typename Comp>
inline T* rbtree<T, C, R>::find_any(const K& key, Comp comp) const {
    T* n = r_.root_;
    while (n) {
        int cmp = comp.compare(key, *n);
        if (cmp == 0)
            break;
        n = n->rblinks_.c_[cmp > 0].node();
    }
    return n;
}

template <typename T, typename C, typename R> template <typename K, typename Comp>
inline T* rbtree<T, C, R>::find_first(const K& key, Comp comp) const {
    T* n = r_.root_;
    T* answer = nullptr;
    while (n) {
        int cmp = comp.compare(key, *n);
        if (cmp == 0)
            answer = n;
        n = n->rblinks_.c_[cmp > 0].node();
    }
    return answer;
}

template <typename T, typename C, typename R>
inline auto rbtree<T, C, R>::find(const_reference& x) const -> const_iterator {
    return find_any(x, rbpriv::make_compare<T, T>(r_.get_compare()));
}

template <typename T, typename C, typename R>
inline auto rbtree<T, C, R>::find(const_reference& x) -> iterator {
    return find_any(x, rbpriv::make_compare<T, T>(r_.get_compare()));
}

template <typename T, typename C, typename R> template <typename K, typename Comp>
inline auto rbtree<T, C, R>::find(const K& key, Comp comp) const -> const_iterator {
    return find_any(key, rbpriv::make_compare<K, T>(comp));
}

template <typename T, typename C, typename R> template <typename K, typename Comp>
inline auto rbtree<T, C, R>::find(const K& key, Comp comp) -> iterator {
    return find_any(key, rbpriv::make_compare<K, T>(comp));
}

template <typename T, typename C, typename R>
inline auto rbtree<T, C, R>::iterator_to(const node_type& x) const -> const_iterator {
    return const_iterator(&x);
}

template <typename T, typename C, typename R>
inline auto rbtree<T, C, R>::iterator_to(node_type& x) -> iterator {
    return iterator(&x);
}

template <typename T, typename C, typename R>
inline auto rbtree<T, C, R>::erase(iterator it) -> iterator {
    rbaccount(erase);
    T* node = it.operator->();
    ++it;
    delete_node(node, it.operator->());
    return it;
}

template <typename T, typename C, typename R>
inline void rbtree<T, C, R>::erase(T& node) {
    rbaccount(erase);
    delete_node(&node, nullptr);
}

template <typename T, typename C, typename R>
inline void rbtree<T, C, R>::erase_and_dispose(T& node) {
    rbaccount(erase);
    delete_node(&node, nullptr);
    delete &node;
}

template <typename T, typename C, typename R> template <typename Disposer>
inline void rbtree<T, C, R>::erase_and_dispose(T& node, Disposer d) {
    rbaccount(erase);
    delete_node(&node, nullptr);
    d(&node);
}

template <typename T, typename C, typename R>
inline T* rbtree<T, C, R>::unlink_leftmost_without_rebalance() {
    T* node = r_.limit_[0];
    if (node) {
        rbnodeptr<T> n(node, false);
        if (n.child(true)) {
            n.child(true).parent() = n.parent();
            r_.limit_[0] = rbalgorithms<T>::edge_node(n.child(true).node(), false);
        } else
            r_.limit_[0] = n.parent();
    } else
        r_.root_ = r_.limit_[1] = 0;
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
                         T* root, T* begin, T* end, C& compare) const {
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
    if (begin && !child(false))
        assert(begin == node());
    if (end && !child(true))
        assert(end == node());
    for (int i = 0; i < 2; ++i)
        if (child(i))
            child(i).check(node(), this_black_height, black_height,
                           root, i ? 0 : begin, i ? end : 0, compare);
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
                                            r_.limit_[0], r_.limit_[1], r_.get_compare());
    else
        assert(!r_.limit_[0] && !r_.limit_[1]);
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
inline T* rbalgorithms<T>::prev_node(T* n, T* last) {
    return n ? step_node(n, false) : last;
}

template <typename T>
inline rbconst_iterator<T>::rbconst_iterator(const T* n)
    : n_(const_cast<T*>(n)) {
}

template <typename T>
inline void rbconst_iterator<T>::operator++(int) {
    n_ = rbalgorithms<T>::next_node(n_);
}

template <typename T>
inline void rbconst_iterator<T>::operator++() {
    n_ = rbalgorithms<T>::next_node(n_);
}

template <typename T>
inline void rbconst_iterator<T>::operator--(int) {
    n_ = rbalgorithms<T>::prev_node(n_);
}

template <typename T>
inline void rbconst_iterator<T>::operator--() {
    n_ = rbalgorithms<T>::prev_node(n_);
}

template <typename T>
inline const T& rbconst_iterator<T>::operator*() const {
    return *n_;
}

template <typename T>
inline const T* rbconst_iterator<T>::operator->() const {
    return n_;
}

template <typename T>
inline rbiterator<T>::rbiterator(T* n)
    : rbconst_iterator<T>(n) {
}

template <typename T>
inline T& rbiterator<T>::operator*() const {
    return *this->n_;
}

template <typename T>
inline T* rbiterator<T>::operator->() const {
    return this->n_;
}

template <typename T>
inline bool operator==(rbconst_iterator<T> a, rbconst_iterator<T> b) {
    return a.operator->() == b.operator->();
}

template <typename T>
inline bool operator!=(rbconst_iterator<T> a, rbconst_iterator<T> b) {
    return a.operator->() != b.operator->();
}

template <typename T, typename R, typename A>
auto rbtree<T, R, A>::begin() const -> const_iterator {
    return const_iterator(r_.limit_[0]);
}

template <typename T, typename R, typename A>
auto rbtree<T, R, A>::begin() -> iterator {
    return iterator(r_.limit_[0]);
}

template <typename T, typename R, typename A>
auto rbtree<T, R, A>::end() const -> const_iterator {
    return const_iterator(nullptr);
}

template <typename T, typename R, typename A>
auto rbtree<T, R, A>::end() -> iterator {
    return iterator(nullptr);
}

template <typename T, typename R, typename A> template <typename K, typename Comp>
inline T* rbtree<T, R, A>::lower_bound_impl(const K& key, Comp comp) const {
    T* n = r_.root_;
    T* bound = 0;
    while (n) {
        bool cmp = comp.greater(key, *n);
        if (!cmp)
            bound = n;
        n = n->rblinks_.c_[cmp].node();
    }
    return bound;
}

template <typename T, typename R, typename A> template <typename K, typename Comp>
inline auto rbtree<T, R, A>::lower_bound(const K& key, Comp comp) const -> const_iterator {
    return lower_bound_impl(key, rbpriv::make_compare<K, T>(comp));
}

template <typename T, typename R, typename A>
inline auto rbtree<T, R, A>::lower_bound(const_reference x) const -> const_iterator {
    return lower_bound_impl(x, rbpriv::make_compare<T, T>(r_.get_compare()));
}

template <typename T, typename R, typename A> template <typename K, typename Comp>
inline auto rbtree<T, R, A>::lower_bound(const K& key, Comp comp) -> iterator {
    return lower_bound_impl(key, rbpriv::make_compare<K, T>(comp));
}

template <typename T, typename R, typename A>
inline auto rbtree<T, R, A>::lower_bound(const_reference x) -> iterator {
    return lower_bound_impl(x, rbpriv::make_compare<T, T>(r_.get_compare()));
}

template <typename T, typename R, typename A> template <typename K, typename Comp>
inline T* rbtree<T, R, A>::upper_bound_impl(const K& key, Comp comp) const {
    T* n = r_.root_;
    T* bound = 0;
    while (n) {
        bool cmp = comp.less(key, *n);
        if (cmp)
            bound = n;
        n = n->rblinks_.c_[!cmp].node();
    }
    return bound;
}

template <typename T, typename R, typename A> template <typename K, typename Comp>
inline auto rbtree<T, R, A>::upper_bound(const K& key, Comp comp) const -> const_iterator {
    return upper_bound_impl(key, rbpriv::make_compare<K, T>(comp));
}

template <typename T, typename R, typename A>
inline auto rbtree<T, R, A>::upper_bound(const_reference x) const -> const_iterator {
    return upper_bound_impl(x, rbpriv::make_compare<T, T>(r_.get_compare()));
}

template <typename T, typename R, typename A> template <typename K, typename Comp>
inline auto rbtree<T, R, A>::upper_bound(const K& key, Comp comp) -> iterator {
    return upper_bound_impl(key, rbpriv::make_compare<K, T>(comp));
}

template <typename T, typename R, typename A>
inline auto rbtree<T, R, A>::upper_bound(const_reference x) -> iterator {
    return upper_bound_impl(x, rbpriv::make_compare<T, T>(r_.get_compare()));
}

#endif
