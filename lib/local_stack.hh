#ifndef GSTORE_LOCAL_STACK_HH
#define GSTORE_LOCAL_STACK_HH 1
#include "compiler.hh"
#include <memory>

template <typename T, int N, typename A = std::allocator<T> >
class local_stack {
  public:
    typedef bool (local_stack<T, N>::*unspecified_bool_type)() const;
    typedef T *iterator;
    typedef const T *const_iterator;

    inline local_stack(const A &allocator = A());
    inline ~local_stack();

    inline int size() const;
    inline bool empty() const;
    inline operator unspecified_bool_type() const;
    inline bool operator!() const;

    inline iterator begin();
    inline iterator end();
    inline const_iterator begin() const;
    inline const_iterator end() const;
    inline const_iterator cbegin() const;
    inline const_iterator cend() const;

    inline T &operator[](int i);
    inline const T &operator[](int i) const;
    inline T &top();
    inline const T &top() const;

    inline void push(const T &x);
    inline void push(T &&x);
    inline void pop();

  private:
    struct rep : public A {
	T *v_;
	int size_;
	int capacity_;
	char lv_[sizeof(T) * N];

	inline rep(const A &a);
    };
    rep r_;

    void grow();
};

template <typename T, int N, typename A>
inline local_stack<T, N, A>::rep::rep(const A &a)
    : A(a), v_(reinterpret_cast<T *>(lv_)), size_(0), capacity_(N) {
}

template <typename T, int N, typename A>
inline local_stack<T, N, A>::local_stack(const A &allocator)
    : r_(allocator) {
}

template <typename T, int N, typename A>
inline local_stack<T, N, A>::~local_stack() {
    for (int i = 0; i < r_.size_; ++i)
	r_.destroy(&r_.v_[i]);
    if (r_.v_ != reinterpret_cast<T *>(r_.lv_))
	r_.deallocate(r_.v_, r_.capacity_);
}

template <typename T, int N, typename A>
inline int local_stack<T, N, A>::size() const {
    return r_.size_;
}

template <typename T, int N, typename A>
inline bool local_stack<T, N, A>::empty() const {
    return r_.size_ == 0;
}

template <typename T, int N, typename A>
inline local_stack<T, N, A>::operator unspecified_bool_type() const {
    return empty() ? 0 : &local_stack<T, N, A>::empty;
}

template <typename T, int N, typename A>
inline bool local_stack<T, N, A>::operator!() const {
    return empty();
}

template <typename T, int N, typename A>
void local_stack<T, N, A>::grow() {
    T *m = r_.allocate(r_.capacity_ * 2);
    for (int i = 0; i < r_.size_; ++i) {
	r_.construct(&m[i], std::move(r_.v_[i]));
	r_.destroy(&r_.v_[i]);
    }
    if (r_.v_ != reinterpret_cast<T *>(r_.lv_))
	r_.deallocate(r_.v_, r_.capacity_);
    r_.v_ = m;
    r_.capacity_ *= 2;
}

template <typename T, int N, typename A>
inline typename local_stack<T, N, A>::iterator local_stack<T, N, A>::begin() {
    return r_.v_;
}

template <typename T, int N, typename A>
inline typename local_stack<T, N, A>::iterator local_stack<T, N, A>::end() {
    return r_.v_ + r_.size_;
}

template <typename T, int N, typename A>
inline typename local_stack<T, N, A>::const_iterator local_stack<T, N, A>::begin() const {
    return r_.v_;
}

template <typename T, int N, typename A>
inline typename local_stack<T, N, A>::const_iterator local_stack<T, N, A>::end() const {
    return r_.v_ + r_.size_;
}

template <typename T, int N, typename A>
inline typename local_stack<T, N, A>::const_iterator local_stack<T, N, A>::cbegin() const {
    return r_.v_;
}

template <typename T, int N, typename A>
inline typename local_stack<T, N, A>::const_iterator local_stack<T, N, A>::cend() const {
    return r_.v_ + r_.size_;
}

template <typename T, int N, typename A>
inline T &local_stack<T, N, A>::operator[](int i) {
    return r_.v_[i];
}

template <typename T, int N, typename A>
inline const T &local_stack<T, N, A>::operator[](int i) const {
    return r_.v_[i];
}

template <typename T, int N, typename A>
inline T &local_stack<T, N, A>::top() {
    return r_.v_[r_.size_ - 1];
}

template <typename T, int N, typename A>
inline const T &local_stack<T, N, A>::top() const {
    return r_.v_[r_.size_ - 1];
}

template <typename T, int N, typename A>
inline void local_stack<T, N, A>::push(const T &x) {
    if (r_.size_ == r_.capacity_)
	grow();
    r_.construct(&r_.v_[r_.size_], x);
    ++r_.size_;
}

template <typename T, int N, typename A>
inline void local_stack<T, N, A>::push(T &&x) {
    if (r_.size_ == r_.capacity_)
	grow();
    r_.construct(&r_.v_[r_.size_], std::move(x));
    ++r_.size_;
}

template <typename T, int N, typename A>
inline void local_stack<T, N, A>::pop() {
    --r_.size_;
}

#endif
