#include <iostream>
#include "rb.hh"
#include "interval.hh"
#include "interval_tree.hh"

template <typename T>
class rbwrapper : public T {
  public:
    template <typename... Args> inline rbwrapper(Args&&... args)
	: T(std::forward<Args>(args)...) {
    }
    inline rbwrapper(const T& x)
	: T(x) {
    }
    inline rbwrapper(T&& x) noexcept
	: T(std::move(x)) {
    }
    inline const T& value() const {
	return *this;
    }
    rblinks<rbwrapper<T> > rblinks_;
};

template <> class rbwrapper<int> {
  public:
    template <typename... Args> inline rbwrapper(int x)
	: x_(x) {
    }
    inline int value() const {
	return x_;
    }
    int x_;
    rblinks<rbwrapper<int> > rblinks_;
};

std::ostream& operator<<(std::ostream& s, rbwrapper<int> x) {
    return s << x.value();
}

template <typename T>
class default_comparator<rbwrapper<T> > {
  public:
    inline int operator()(const rbwrapper<T> &a, const rbwrapper<T> &b) const {
	return default_compare(a.value(), b.value());
    }
    inline int operator()(const rbwrapper<T> &a, const T &b) const {
	return default_compare(a.value(), b);
    }
    inline int operator()(const T &a, const rbwrapper<T> &b) const {
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
    int_interval(int first, int last)
	: interval<int>(first, last), subtree_iend_(iend()) {
    }
    int subtree_iend() const {
	return subtree_iend_;
    }
    void set_subtree_iend(int i) {
	subtree_iend_ = i;
    }
};

int main(int argc, char **argv) {
    if (1) {
	const int N = 50000;
	rbtree<rbwrapper<int> > tree;
	int *x = new int[N];
	for (int i = 0; i < N; ++i)
	    x[i] = i;
	for (int i = 0; i < N; ++i) {
	    int j = random() % (N - i);
	    int val = x[j];
	    x[j] = x[N - i - 1];
	    tree.insert(new rbwrapper<int>(val));
	}
	std::cerr << tree << "\n\n";
	for (int i = 0; i < N; ++i)
	    x[i] = i;
	for (int i = 0; i < N; ++i) {
	    int j = random() % (N - i);
	    int val = x[j];
	    x[j] = x[N - i - 1];
	    tree.erase_and_dispose(tree.find(rbwrapper<int>(val)));
	    //if (i % 1000 == 999) std::cerr << "\n\n" << i << "\n" << tree << "\n\n";
	}
	std::cerr << tree << "\n\n";
	delete[] x;
    }

    {
	rbtree<rbwrapper<int> > tree;
	tree.insert(new rbwrapper<int>(0));
	auto x = new rbwrapper<int>(1);
	tree.insert(x);
	tree.insert(new rbwrapper<int>(0));
	tree.insert(new rbwrapper<int>(-2));
	auto y = new rbwrapper<int>(0);
	tree.insert(y);
	std::cerr << tree << "\n";
	tree.erase_and_dispose(x);
	std::cerr << tree << "\n";
	tree.erase_and_dispose(y);
	std::cerr << tree << "\n";
    }

    interval_tree<rbwrapper<int_interval> > tree;
    for (int i = 0; i < 100; ++i) {
	int a = random() % 1000;
	tree.insert(new rbwrapper<int_interval>(a, a + random() % 200));
    }
    std::cerr << tree << "\n\n";
    tree.visit_overlaps(40, print);
    std::cerr << "\n";
    tree.visit_overlaps(interval<int>(10, 30), print);
}
