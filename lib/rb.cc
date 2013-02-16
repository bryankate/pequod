#include "config.h"
#include <iostream>
#include "xinterval.hh"
#include "xintervaltree.hh"

template <typename T>
class wrapper {
  public:
    inline wrapper(const T &x)
	: x_(x) {
    }
    inline wrapper(T &&x) noexcept
	: x_(std::move(x)) {
    }
    inline T &value() {
	return x_;
    }
    inline const T &value() const {
	return x_;
    }
    inline operator T &() {
	return x_;
    }
    inline operator const T &() const {
	return x_;
    }
    inline wrapper<T> &operator=(const T &x) {
	x_ = x;
	return *this;
    }
    inline wrapper<T> &operator=(T &&x) {
	x_ = std::move(x);
	return *this;
    }
  private:
    T x_;
};

template <typename T>
class default_comparator<wrapper<T> > {
  public:
    inline int operator()(const wrapper<T> &a, const wrapper<T> &b) const {
	return default_compare(a.value(), b.value());
    }
    inline int operator()(const wrapper<T> &a, const T &b) const {
	return default_compare(a.value(), b);
    }
    inline int operator()(const T &a, const wrapper<T> &b) const {
	return default_compare(a, b.value());
    }
    inline int operator()(const T &a, const T &b) const {
	return default_compare(a, b);
    }
};

void print(interval<int> &x) {
    std::cerr << x << "\n";
}

template <typename A, typename B>
struct semipair {
    A first;
    B second;
    inline semipair() {
    }
    inline semipair(const A &a)
	: first(a) {
    }
    inline semipair(const A &a, const B &b)
	: first(a), second(b) {
    }
    inline semipair(const semipair<A, B> &x)
	: first(x.first), second(x.second) {
    }
    template <typename X, typename Y> inline semipair(const semipair<X, Y> &x)
	: first(x.first), second(x.second) {
    }
    template <typename X, typename Y> inline semipair(const std::pair<X, Y> &x)
	: first(x.first), second(x.second) {
    }
    inline semipair<A, B> &operator=(const semipair<A, B> &x) {
	first = x.first;
	second = x.second;
	return *this;
    }
    template <typename X, typename Y>
    inline semipair<A, B> &operator=(const semipair<X, Y> &x) {
	first = x.first;
	second = x.second;
	return *this;
    }
    template <typename X, typename Y>
    inline semipair<A, B> &operator=(const std::pair<X, Y> &x) {
	first = x.first;
	second = x.second;
	return *this;
    }
};

template <typename A, typename B>
std::ostream &operator<<(std::ostream &s, const semipair<A, B> &x) {
    return s << '<' << x.first << ", " << x.second << '>';
}

struct compare_first {
    template <typename T, typename U>
    inline int operator()(const T &a, const std::pair<T, U> &b) const {
	return default_compare(a, b.first);
    }
    template <typename T, typename U, typename V>
    inline int operator()(const std::pair<T, U> &a, const std::pair<T, V> &b) const {
	return default_compare(a.first, b.first);
    }
    template <typename T, typename U>
    inline int operator()(const T &a, const semipair<T, U> &b) const {
	return default_compare(a, b.first);
    }
    template <typename T, typename U, typename V>
    inline int operator()(const semipair<T, U> &a, const semipair<T, V> &b) const {
	return default_compare(a.first, b.first);
    }
};

struct int_interval : public interval<int> {
    int subtree_iend_;
    int_interval(const interval<int> &x)
	: interval<int>(x), subtree_iend_(x.iend()) {
    }
    int subtree_iend() const {
	return subtree_iend_;
    }
    void set_subtree_iend(int i) {
	subtree_iend_ = i;
    }
};

int main(int argc, char **argv) {
    if (0) {
	const int N = 5000;
	rbtree<wrapper<int> > tree;
	int *x = new int[N];
	for (int i = 0; i < N; ++i)
	    x[i] = i;
	for (int i = 0; i < N; ++i) {
	    int j = random() % (N - i);
	    int val = x[j];
	    x[j] = x[N - i - 1];
	    tree.insert(wrapper<int>(val));
	}
	std::cerr << tree << "\n\n";
	for (int i = 0; i < N; ++i)
	    x[i] = i;
	for (int i = 0; i < N; ++i) {
	    int j = random() % (N - i);
	    int val = x[j];
	    x[j] = x[N - i - 1];
	    tree.erase(tree.find(wrapper<int>(val)));
	    if (i % 1000 == 999) std::cerr << "\n\n" << i << "\n" << tree << "\n\n";
	}
	std::cerr << tree << "\n\n";
	delete[] x;
    }

    {
	rbtree<wrapper<int> > tree;
	tree.insert(wrapper<int>(0));
	auto x = tree.insert(wrapper<int>(1));
	tree.insert(wrapper<int>(0));
	tree.insert(wrapper<int>(-2));
	auto y = tree.insert(wrapper<int>(0));
	std::cerr << tree << "\n";
	tree.erase(x);
	std::cerr << tree << "\n";
	tree.erase(y);
	std::cerr << tree << "\n";
    }

    {
	rbtree<semipair<int, int>, compare_first> tree;
	tree[0] = std::make_pair(0, 2);
	tree[1] = std::make_pair(1, 3);
	tree[1].second = 4;
	tree[-1].second = 5;
	std::cerr << tree << "\n";
    }

    interval_tree<int_interval> tree;
    for (int i = 0; i < 100; ++i) {
	int a = random() % 1000;
	tree.insert(interval<int>(a, a + random() % 200));
    }
    std::cerr << tree << "\n\n";
    tree.visit_overlaps(40, print);
    std::cerr << "\n";
    tree.visit_overlaps(interval<int>(10, 30), print);
}
