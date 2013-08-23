dnl Process this file with autoconf to produce a configure script.

AC_CONFIG_AUX_DIR(tamer)
AC_PREREQ(2.60)

AC_INIT([pequod], [0.1])
AC_CONFIG_HEADERS([config.h])
AC_CONFIG_FILES([GNUmakefile])
AC_CONFIG_SUBDIRS([tamer])
AC_SUBST(ac_configure_args)

ac_user_cxx=${CXX+y}
ac_user_cxxflags=${CXXFLAGS+y}

AC_PROG_CC
AC_PROG_CXX
AC_PROG_INSTALL
AC_LANG_CPLUSPLUS

AC_C_BIGENDIAN([AC_DEFINE([ENDIAN_BIG], [1], [big endian])],
               [AC_DEFINE([ENDIAN_LITTLE], [1], [little endian])],
               [AC_MSG_ERROR([Unknown endian])],
               [AC_MSG_ERROR([Universal endian])])

AC_CHECK_HEADERS([sys/epoll.h numa.h ev++.h])
if test -z "$ac_cv_header_evpp_h"; then
    AC_MSG_ERROR([Cannot find <ev++.h>])
fi

AC_SEARCH_LIBS([numa_available], [numa], [AC_DEFINE([HAVE_LIBNUMA], [1], [Define if you have libnuma.])])

AC_CHECK_HEADERS([memcached/protocol_binary.h])

AC_CHECK_HEADERS([hiredis/hiredis.h], [HIREDIS_LIB="-lhiredis"], [HIREDIS_LIB=])
AC_SUBST([HIREDIS_LIB])

POSTGRES_LIBS=
AC_CHECK_HEADERS([postgresql/libpq-fe.h], [AC_DEFINE([HAVE_LIBPQ], [1], [Define if you have libpq.])])
AC_CHECK_HEADERS([libpq-fe.h], [AC_DEFINE([HAVE_LIBPQ], [1], [Define if you have libpq.])])
AC_CHECK_LIB([pq], [PQuser], [POSTGRES_LIBS="-lpq"], [ac_cv_have_postgres=no])
AC_SUBST([POSTGRES_LIBS])

AC_ARG_WITH([pqxx],
    [AS_HELP_STRING([--with-pqxx],
    [enable postgres backend if available])],
    [], [with_pqxx=no])
PQXX_LIB=
if test "x$with_pqxx" != "xno"; then
    if test "$ac_cv_have_postgres" = no; then
        AC_MSG_ERROR([libpqxx requires libpq])
    fi
    AC_CHECK_HEADERS([pqxx/pqxx pqxx/notification], [], AC_MSG_ERROR([missing libpqxx headers]))
    # test for pqxx library >= 4.0 by looking for transactor class
    AC_LANG_PUSH(C++)
    SAVED_LDFLAGS=$LDFLAGS
    LDFLAGS="$LDFLAGS -lpqxx"
    AC_LINK_IFELSE([AC_LANG_PROGRAM([#include <pqxx/transactor.hxx>], [pqxx::transactor<> dummy;])], 
                   [PQXX_LIB="-lpqxx"], AC_MSG_ERROR([libqxx >= 4.0 required.]))
    LDFLAGS=$SAVED_LDFLAGS
    AC_LANG_POP(C++)
fi
AC_SUBST([PQXX_LIB])


dnl Builtins

AC_DEFUN([KVDB_CHECK_BUILTIN], [
    AC_CACHE_CHECK([for $1 builtin], [ac_cv_have_$1],
	[AC_LINK_IFELSE([AC_LANG_PROGRAM([$2], [])],
	    [ac_cv_have_$1=yes], [ac_cv_have_$1=no])])
    if test $ac_cv_have_$1 = yes; then
	AC_DEFINE(AS_TR_CPP([HAVE_$1]), [1], [Define if you have the $1 builtin.])
    fi
])

KVDB_CHECK_BUILTIN([__builtin_clz],
    [[unsigned f(unsigned x) { return __builtin_clz(x); }]])

KVDB_CHECK_BUILTIN([__builtin_clzl],
    [[unsigned long f(unsigned long x) { return __builtin_clzl(x); }]])

KVDB_CHECK_BUILTIN([__builtin_clzll],
    [[unsigned long long f(unsigned long long x) { return __builtin_clzll(x); }]])

KVDB_CHECK_BUILTIN([__builtin_ctz],
    [[unsigned f(unsigned x) { return __builtin_ctz(x); }]])

KVDB_CHECK_BUILTIN([__builtin_ctzl],
    [[unsigned long f(unsigned long x) { return __builtin_ctzl(x); }]])

KVDB_CHECK_BUILTIN([__builtin_ctzll],
    [[unsigned long long f(unsigned long long x) { return __builtin_ctzll(x); }]])

KVDB_CHECK_BUILTIN([__sync_synchronize], [[long x = 11;
    void f(long i) { long* y = &x; __sync_synchronize(); *y = i; }]])

KVDB_CHECK_BUILTIN([__sync_fetch_and_add],
    [[long f(long* x) { return __sync_fetch_and_add(x, 2L); }]])

KVDB_CHECK_BUILTIN([__sync_fetch_and_add_8],
    [[#include <stdint.h>
    int64_t f(int64_t* x) { return __sync_fetch_and_add(x, (int64_t) 2); }]])

KVDB_CHECK_BUILTIN([__sync_bool_compare_and_swap],
    [[bool f(long* x, long y, long z) { return __sync_bool_compare_and_swap(x, y, z); }]])

KVDB_CHECK_BUILTIN([__sync_bool_compare_and_swap_8],
    [[#include <stdint.h>
    bool f(int64_t* x, int64_t y, int64_t z) { return __sync_bool_compare_and_swap(x, y, z); }]])

KVDB_CHECK_BUILTIN([__sync_val_compare_and_swap],
    [[long f(long* x, long y, long z) { return __sync_val_compare_and_swap(x, y, z); }]])

KVDB_CHECK_BUILTIN([__sync_val_compare_and_swap_8],
    [[#include <stdint.h>
    int64_t f(int64_t* x, int64_t y, int64_t z) { return __sync_val_compare_and_swap(x, y, z); }]])

KVDB_CHECK_BUILTIN([__sync_lock_test_and_set],
    [[long f(long* x) { return __sync_lock_test_and_set(x, 1); }]])

KVDB_CHECK_BUILTIN([__sync_lock_test_and_set_val],
    [[long f(long* x, long y) { return __sync_lock_test_and_set(x, y); }]])

KVDB_CHECK_BUILTIN([__sync_lock_release_set],
    [[void f(long* x) { __sync_lock_release(x); }]])


dnl C++ features

AC_CACHE_CHECK([whether the C++ compiler understands 'auto'], [ac_cv_cxx_auto], [
    AC_COMPILE_IFELSE([AC_LANG_PROGRAM([[struct s { int a; }; int f(s x) { auto &y = x; return y.a; }]], [[]])],
	[ac_cv_cxx_auto=yes], [ac_cv_cxx_auto=no])])
if test "$ac_cv_cxx_auto" != yes -a -z "$ac_user_cxx"; then
    CXX="${CXX} -std=gnu++0x"
    AC_MSG_CHECKING([whether the C++ compiler with -std=gnu++0x understands 'auto'])
    AC_COMPILE_IFELSE([AC_LANG_PROGRAM([[struct s { int a; }; int f(s x) { auto &y = x; return y.a; }]], [[]])],
	[ac_cv_cxx_auto=yes], [ac_cv_cxx_auto=no])
    AC_MSG_RESULT([$ac_cv_cxx_auto])
fi

if test "$ac_cv_cxx_auto" = yes; then
    AC_DEFINE([HAVE_CXX_AUTO], [1], [Define if the C++ compiler understands 'auto'.])
else
    AC_MSG_ERROR([

The C++ compiler does not appear to understand C++11.
To fix this problem, try supplying a "CXX" argument to ./configure,
such as "./configure CXX='c++ -std=gnu++0x'".

========================================================])
fi

AC_CACHE_CHECK([whether the C++ compiler understands constexpr], [ac_cv_cxx_constexpr], [
    AC_COMPILE_IFELSE([AC_LANG_PROGRAM([[constexpr int f(int x) { return x + 1; }]], [[]])],
	[ac_cv_cxx_constexpr=yes], [ac_cv_cxx_constexpr=no])])
if test "$ac_cv_cxx_constexpr" = yes; then
    AC_DEFINE([HAVE_CXX_CONSTEXPR], [1], [Define if the C++ compiler understands constexpr.])
fi

AC_CACHE_CHECK([whether the C++ compiler understands static_assert], [ac_cv_cxx_static_assert], [
    AC_COMPILE_IFELSE([AC_LANG_PROGRAM([[const int f = 2;]], [[static_assert(f == 2, "f should be 2");]])],
	[ac_cv_cxx_static_assert=yes], [ac_cv_cxx_static_assert=no])])
if test "$ac_cv_cxx_static_assert" = yes; then
    AC_DEFINE([HAVE_CXX_STATIC_ASSERT], [1], [Define if the C++ compiler understands static_assert.])
fi

AC_CACHE_CHECK([whether the C++ compiler understands rvalue references], [ac_cv_cxx_rvalue_references], [
    AC_COMPILE_IFELSE([AC_LANG_PROGRAM([[int f(int &) { return 1; } int f(int &&) { return 0; }]], [[return f(int());]])],
	[ac_cv_cxx_rvalue_references=yes], [ac_cv_cxx_rvalue_references=no])])
if test "$ac_cv_cxx_rvalue_references" = yes; then
    AC_DEFINE([HAVE_CXX_RVALUE_REFERENCES], [1], [Define if the C++ compiler understands rvalue references.])
fi

AC_CACHE_CHECK([whether the C++ compiler understands template alias], [ac_cv_cxx_template_alias], [
    AC_COMPILE_IFELSE([AC_LANG_PROGRAM([[template <typename T> struct X { typedef T type; }; template <typename T> using Y = X<T>; int f(int x) { return x; }]], [[return f(Y<int>::type());]])],
	[ac_cv_cxx_template_alias=yes], [ac_cv_cxx_template_alias=no])])
if test "$ac_cv_cxx_template_alias" = yes; then
    AC_DEFINE([HAVE_CXX_TEMPLATE_ALIAS], [1], [Define if the C++ compiler understands template alias.])
fi

AC_CACHE_CHECK([whether the C++ compiler understands user-defined literals], [ac_cv_cxx_user_literals], [
    AC_COMPILE_IFELSE([AC_LANG_PROGRAM([[#include <stddef.h>
size_t operator"" LEN(const char *, size_t len) { return len; } int f(size_t x) { return x > 2; }]], [[return f("ABC"LEN);]])],
	[ac_cv_cxx_user_literals=yes], [ac_cv_cxx_user_literals=no])])
if test "$ac_cv_cxx_user_literals" = yes; then
    AC_DEFINE([HAVE_CXX_USER_LITERALS], [1], [Define if the C++ compiler understands user-defined literals.])
fi

AC_CHECK_HEADERS([type_traits])

AC_CACHE_CHECK([for __has_trivial_copy], [ac_cv_have___has_trivial_copy], [
    AC_COMPILE_IFELSE([AC_LANG_PROGRAM([], [[long x = 1; if (__has_trivial_copy(long)) x = 0;]])], [ac_cv_have___has_trivial_copy=yes], [ac_cv_have___has_trivial_copy=no])])
if test $ac_cv_have___has_trivial_copy = yes; then
    AC_DEFINE([HAVE___HAS_TRIVIAL_COPY], [1], [Define if you have the __has_trivial_copy compiler intrinsic.])
fi

if test "$ac_cv_cxx_rvalue_references" = yes; then
    AC_MSG_CHECKING([for std::move])
    AC_COMPILE_IFELSE([AC_LANG_PROGRAM([#include <utility>], [[long x = 0; long &&y = std::move(x);]])], [ac_cv_std_move=yes], [ac_cv_std_move=no])
    AC_MSG_RESULT([$ac_cv_std_move])
    if test "$ac_cv_std_move" != yes; then
        AC_MSG_ERROR([

The C++ compiler understands C++11, but does not have std::move.
If you are using clang on Mac, ensure the -stdlib=libc++ option.

========================================================])
    fi
fi

AC_CACHE_CHECK([for std::is_trivially_copyable], [ac_cv_have_std_is_trivially_copyable], [
    AC_COMPILE_IFELSE([AC_LANG_PROGRAM([#include <type_traits>], [[return std::is_trivially_copyable<int>::value;]])], [ac_cv_have_std_is_trivially_copyable=yes], [ac_cv_have_std_is_trivially_copyable=no])])
if test $ac_cv_have_std_is_trivially_copyable = yes; then
    AC_DEFINE([HAVE_STD_IS_TRIVIALLY_COPYABLE], [1], [Define if you have the std::is_trivially_copyable template.])
fi

AC_CACHE_CHECK([for std::is_rvalue_reference], [ac_cv_have_std_is_rvalue_reference], [
    AC_COMPILE_IFELSE([AC_LANG_PROGRAM([#include <type_traits>], [[return std::is_rvalue_reference<int>::value;]])], [ac_cv_have_std_is_rvalue_reference=yes], [ac_cv_have_std_is_rvalue_reference=no])])
if test $ac_cv_have_std_is_rvalue_reference = yes; then
    AC_DEFINE([HAVE_STD_IS_RVALUE_REFERENCE], [1], [Define if you have the std::is_rvalue_reference template.])
fi


dnl Memory allocator

AC_CHECK_LIB([flow], [malloc], [have_flow=true], [have_flow=])
AC_CHECK_LIB([jemalloc], [mallctl], [have_jemalloc=true], [have_jemalloc=])
if test "x$have_jemalloc" != "xtrue"; then
    AC_CHECK_LIB([jemalloc], [je_mallctl], [have_jemalloc=true], [have_jemalloc=])
fi
AC_CHECK_LIB([tcmalloc], [tc_malloc], [have_tcmalloc=true], [have_tcmalloc=])
AC_CHECK_LIB([tcmalloc_minimal], [tc_malloc], [have_tcmalloc_minimal=true], [have_tcmalloc_minimal=])

AC_ARG_WITH([malloc],
    [AS_HELP_STRING([--with-malloc=TYPE],
                    [Memory allocator (malloc|jemalloc|tcmalloc|flow)])],
    [ac_kvdb_malloc=$withval], [ac_kvdb_malloc=yes])

if test \( "$ac_kvdb_malloc" = tcmalloc -a -z "$have_tcmalloc_minimal" -a -z "$have_tcmalloc" \) \
	-o \( "$ac_kvdb_malloc" = jemalloc -a -z "$have_jemalloc" \) \
	-o \( "$ac_kvdb_malloc" = flow -a -z "$have_flow" \); then
    AC_MSG_ERROR([$ac_kvdb_malloc not found])
elif test "$ac_kvdb_malloc" = tcmalloc -o "$ac_kvdb_malloc" = jemalloc -o "$ac_kvdb_malloc" = flow; then
    :
elif test "$ac_kvdb_malloc" = yes -o "$ac_kvdb_malloc" = default; then
    AC_MSG_CHECKING([for malloc library])
    if test -n "$have_jemalloc"; then ac_kvdb_malloc=jemalloc;
    elif test -n "$have_tcmalloc_minimal"; then ac_kvdb_malloc=tcmalloc;
    else ac_kvdb_malloc=malloc; fi
    AC_MSG_RESULT([$ac_kvdb_malloc])
elif test "$ac_kvdb_malloc" = no -o "$ac_kvdb_malloc" = malloc -o -z "$ac_kvdb_malloc"; then
    ac_kvdb_malloc=malloc
else
    AC_MSG_ERROR([Unknown malloc type $ac_kvdb_malloc])
fi

MALLOC_LIBS=
DEFAULT_MALLOC_ASSIGNMENT=
if test "$ac_kvdb_malloc" = tcmalloc; then
    if test -n "$have_tcmalloc"; then
	MALLOC_LIBS="-ltcmalloc"
    else
	MALLOC_LIBS="-ltcmalloc_minimal"
    fi
    AC_DEFINE([HAVE_TCMALLOC], [1], [Define if you are using libtcmalloc for malloc.])
elif test "$ac_kvdb_malloc" = jemalloc; then
    MALLOC_LIBS="-ljemalloc"
    AC_DEFINE([HAVE_JEMALLOC], [1], [Define if you are using libjemalloc for malloc.])
elif test "$ac_kvdb_malloc" = flow; then
    MALLOC_LIBS="-lflow"
    AC_DEFINE([HAVE_FLOW_MALLOC], [1], [Define if you are using libflow for malloc.])
else
    DEFAULT_MALLOC_ASSIGNMENT="DEFAULT_MALLOC = 1"
fi
AC_SUBST([MALLOC_LIBS])
AC_SUBST([DEFAULT_MALLOC_ASSIGNMENT])

dnl Types

AC_DEFUN([KVDB_CHECK_SAME_TYPE], [
    pushdef([KVDB_CST_VAR], [AS_TR_SH([ac_cv_have_same_type_$1_is_$2])])
    AC_CACHE_CHECK([whether $1 and $2 are the same type], KVDB_CST_VAR,
	[AC_COMPILE_IFELSE([AC_LANG_PROGRAM([$3
int f($1) {return 0;} int f($2) {return 0;}], [])],
	    [KVDB_CST_VAR=no], [KVDB_CST_VAR=yes])])
    if test $KVDB_CST_VAR = yes; then
	AC_DEFINE(AS_TR_CPP([HAVE_$1_IS_$2]), [1], [Define if $1 and $2 are the same type.])
    fi
    popdef([KVDB_CST_VAR])
])

KVDB_CHECK_SAME_TYPE([off_t], [long], [#include <stdio.h>])
KVDB_CHECK_SAME_TYPE([off_t], [long long], [#include <stdio.h>])
KVDB_CHECK_SAME_TYPE([int64_t], [long], [#include <stdint.h>])
KVDB_CHECK_SAME_TYPE([int64_t], [long long], [#include <stdint.h>])
KVDB_CHECK_SAME_TYPE([size_t], [unsigned], [#include <stdio.h>])
KVDB_CHECK_SAME_TYPE([size_t], [unsigned long], [#include <stdio.h>])
KVDB_CHECK_SAME_TYPE([size_t], [unsigned long long], [#include <stdio.h>])

AC_CHECK_TYPES([long long])
AC_CHECK_SIZEOF([short])
AC_CHECK_SIZEOF([int])
AC_CHECK_SIZEOF([long])
AC_CHECK_SIZEOF([long long])
AC_CHECK_SIZEOF([void *])

AC_CHECK_DECLS([getline])

AC_CHECK_HEADERS([time.h])
AC_CHECK_DECLS([clock_gettime], [], [], [#if HAVE_TIME_H
# include <time.h>
#endif])
AC_SEARCH_LIBS([clock_gettime], [rt])
AC_CHECK_FUNCS([clock_gettime])

AC_ARG_ENABLE([superpage],
    [AS_HELP_STRING([--disable-superpage],
	    [Disable superpage support])],
    [], [enable_superpage=yes])
if test "$enable_superpage" != no; then
    AC_DEFINE_UNQUOTED([HAVE_SUPERPAGE_ENABLED], [1], [Define if superpage support is enabled.])
fi

AC_ARG_ENABLE([hint],
    [AS_HELP_STRING([--disable-hint],
	    [Disable hint support])],
    [], [enable_hint=yes])
if test "$enable_hint" != no; then
    AC_DEFINE_UNQUOTED([HAVE_HINT_ENABLED], [1], [Define if hint support is enabled.])
fi

AC_ARG_ENABLE([value_sharing],
    [AS_HELP_STRING([--disable-value-sharing],
	    [Disable value sharing support])],
    [], [enable_value_sharing=yes])
if test "$enable_value_sharing" != no; then
    AC_DEFINE_UNQUOTED([HAVE_VALUE_SHARING_ENABLED], [1], [Define if value sharing support is enabled.])
fi

AC_ARG_ENABLE([glog],
    [AS_HELP_STRING([--enable-glog[[=N]]], [Enable gstore_server logging])],
    [], [enable_glog=0])
if test "$enable_glog" = yes -o "$enable_glog" = y; then
    enable_glog=2
fi
if expr "$enable_glog" : "[[0-9]]*" >/dev/null 2>&1; then
    AC_DEFINE_UNQUOTED([GLOGGING], [$enable_glog], [Define to non-zero to enable gstore_server logging.])
fi

AC_ARG_ENABLE([pprof],
    [AS_HELP_STRING([--enable-pprof], [Enable profiling with pprof])],
    [], [enable_pprof=no])
if test "$enable_pprof" = yes; then
    AC_CHECK_HEADERS([gperftools/profiler.h gperftools/heap-profiler.h], [], 
                     [AC_MSG_ERROR([Cannot find <gperftools/profiler.h> or <gperftools/heap-profiler.h>, must --disable-pprof])])
    AC_CHECK_DECLS([ProfilerStart], [], [AC_MSG_ERROR([Cannot find ProfilerStart declaration, must --disable-pprof])], [#include <gperftools/profiler.h>])
    AC_CHECK_DECLS([HeapProfilerStart], [], [AC_MSG_ERROR([Cannot find HeapProfilerStart declaration, must --disable-pprof])], [#include <gperftools/heap-profiler.h>])

    MALLOC_LIBS="$MALLOC_LIBS -lprofiler"
    AC_DEFINE_UNQUOTED([HAVE_PPROF], [1], [Define to enable pprof profiling.])
fi

AC_ARG_ENABLE([timing],
    [AS_HELP_STRING([--enable-timing], [Enable server side timing of operations.])],
    [], [enable_timing=0])
if test "$enable_timing" = yes; then
    AC_DEFINE_UNQUOTED([TIMING], [1], [Define to enable server side timing.])
fi

AC_ARG_ENABLE([tamer-debug],
    [AS_HELP_STRING([--enable-tamer-debug],
	    [Enable extra tamer debugging])],
    [], [enable_tamer_debug=no])
TAMERFLAGS=
if test "$enable_tamer_debug" != no; then
    TAMERFLAGS=-g
fi
AC_SUBST([TAMERFLAGS])

AC_DEFINE_UNQUOTED([HAVE_UNALIGNED_ACCESS], [1], [Define if unaligned accesses are OK.])

dnl
dnl CLICK_CHECK_ADDRESSABLE_VA_LIST
dnl Checks whether the va_list type is addressable.
dnl

AC_DEFUN([CLICK_CHECK_ADDRESSABLE_VA_LIST], [
    AC_LANG_CPLUSPLUS
    AC_CACHE_CHECK([for addressable va_list type], [ac_cv_va_list_addr],
        [AC_COMPILE_IFELSE([AC_LANG_PROGRAM([[#include <stdarg.h>
void f(va_list *) {
}
void g(va_list val) {
    f(&val);
}
void h(int a, ...) {
    va_list val;
    va_start(val, a);
    g(val);
    va_end(val);
}]], [[h(2, 3, 4);]])], ac_cv_va_list_addr=yes, ac_cv_va_list_addr=no)])
    if test "x$ac_cv_va_list_addr" = xyes; then
        AC_DEFINE([HAVE_ADDRESSABLE_VA_LIST], [1], [Define if the va_list type is addressable.])
    fi
])

CLICK_CHECK_ADDRESSABLE_VA_LIST

dnl check for presence of required libraries

AC_CHECK_LIB([ev], [ev_backend], [:], [AC_MSG_ERROR([libev not found])])
AC_CHECK_HEADER([boost/accumulators/accumulators.hpp], [:], [AC_MSG_ERROR([<boost/accumulators/accumulators.hpp> not found])])
AC_CHECK_HEADER([boost/random.hpp], [:], [AC_MSG_ERROR([<boost/random.hpp> not found])])
AC_CHECK_HEADER([boost/intrusive/set.hpp], [:], [AC_MSG_ERROR([<boost/intrusive/set.hpp> not found])])

AC_CHECK_LIB([boost_system-mt], [_ZTIN5boost6system14error_categoryE],
	[BOOST_LIBS="-lboost_system-mt -lboost_thread-mt"],
  [AC_CHECK_LIB([boost_system], [_ZTIN5boost6system14error_categoryE],
	[BOOST_LIBS="-lboost_system -lboost_thread"],
        [AC_MSG_ERROR([Usable -lboost_system not found])])])
AC_SUBST([BOOST_LIBS])

AC_OUTPUT
