#! /usr/bin/perl


my %knowntypes = ("bytes" => 1, "uint32" => 1, "bool" => 1,
		  "uint64" => 1, "int32" => 1, "int64" => 1,
		  "double" => 1);
my %typemap = ("uint32" => "uint32_t", "uint64" => "uint64_t",
	       "int32" => "int32_t", "int64" => "int64_t",
	       "bytes" => "S");
my %fastargmap = ("S" => "typename S::argument_type");


my($cpp_out, $file);
while (@ARGV) {
    if ($ARGV[0] =~ m{^--cpp_out=(.*)}) {
	$cpp_out = $1;
    } elsif (/^-./) {
	print STDERR "Usage: protox.pl [--cpp_out=DIR]\n";
	exit 1;
    } else {
	die "file redefined" if defined($file);
	$file = $ARGV[0];
    }
    shift;
}


sub define_enum ($$\%) {
    my($ename, $t, $defs) = @_;
    my(%enum, @ef);
    while (1) {
	$t =~ s{\A\s+}{};
	if ($t =~ m{\A(\w+)\s*=\s*([-+]?\d+)\s*;?(.*)\z}ms) {
	    die "enum $ename: $1 already defined" if exists($defs->{$1});
	    $defs->{$1} = 1;
	    $enum{$1} = +$2;
	    push @ef, [$1, +$2];
	    $t = $3;
	} elsif ($t eq "") {
	    last;
	} else {
	    die "enum: what? " . substr($t, 0, 100);
	}
    }
    my(%o) = ("type" => "enum", "name" => $ename, "enum" => \%enum,
	      "f" => \@ef);
    $defs->{$ename} = \%o;
    return \%o;
}

sub define_message ($$\%) {
    my($mname, $t, $defs) = @_;
    my(@fields, %ldefs);
    my($anystring) = 0;
    while (1) {
	$t =~ s{\A\s+}{};
	my($ki, $ty, $na, $va, $de);
	if ($t =~ m{\Arequired\s*(\w+)\s*(\w+)\s*=\s*(\d+)\s*;?(.*)\z}ms) {
	    ($ki, $ty, $na, $va, $de, $t) = (0, $1, $2, $3, undef, $4);
	} elsif ($t =~ m{\Aoptional\s*(\w+)\s*(\w+)\s*=\s*(\d+)\s*\[\s*default\s*=\s*(\w+)\s*\]\s*;?(.*)\z}ms) {
	    ($ki, $ty, $na, $va, $de, $t) = (1, $1, $2, $3, $4, $5);
	} elsif ($t =~ m{\Aoptional\s*(\w+)\s*(\w+)\s*=\s*(\d+)\s*;?(.*)\z}ms) {
	    ($ki, $ty, $na, $va, $de, $t) = (1, $1, $2, $3, undef, $4);
	} elsif ($t =~ m{\Arepeated\s*(\w+)\s*(\w+)\s*=\s*(\d+)\s*;?(.*)\z}ms) {
	    ($ki, $ty, $na, $va, $de, $t) = (2, $1, $2, $3, undef, $4);
	} elsif ($t eq "") {
	    last;
	} else {
	    die "message: what? " . substr($t, 0, 100);
	}
	die "message $mname: repeated field $na" if exists($ldefs{$na});
	die "message $mname: repeated field $va" if exists($ldefs{$va});
	die "message $mname: $na: unknown type $ty" if !exists($knowntypes{$ty}) && (!exists($defs->{$ty}) || $defs->{$ty}->{"type"} ne "enum");
	$ty = $typemap{$ty} if exists($typemap{$ty});
	$anystring = 1 if $ty eq "S";
	my($argty) = $ty;
	$argty = $fastargmap{$ty} if exists($fastargmap{$ty});
	my($cast) = "";
	$cast = "int" if exists($defs->{$ty});
	my(%m) = ("name" => $na, "field" => $va, "default" => $de,
		  "kind" => $ki, "type" => $ty, "argtype" => $argty,
		  "id" => $va, "cast" => $cast);
	$fields[$va] = \%m;
    }
    my(%o) = ("type" => "message", "name" => $mname, "f" => \@fields,
	      "anystring" => $anystring);
    $defs->{$mname} = \%o;
    return \%o;
}

sub parse_file ($) {
    my($t) = @_;
    $t =~ s{//.*$}{}mg;
    $t =~ s{/\*.*?\*/}{}msg;
    my(%e, %defs);
    while (1) {
	$t =~ s{\A\s+}{};
	if ($t =~ m{\Apackage\s+(\w+)\s*;?(.*)\z}ms) {
	    die "package already defined" if $e{"__pkg__"};
	    $e{"__pkg__"} = $1;
	    $t = $2;
	} elsif ($t =~ m{\Aenum\s+(\w+)\s*\{(.*?)\}(.*)\z}ms) {
	    die "$1 already defined" if exists($defs{$1});
	    $t = $3;
	    $e{$1} = define_enum($1, $2, %defs);
	} elsif ($t =~ m{\Amessage\s+(\w+)\s*\{(.*?)\}(.*)\z}ms) {
	    die "$1 already defined" if exists($defs{$1});
	    $t = $3;
	    $e{$1} = define_message($1, $2, %defs);
	} elsif ($t eq "") {
	    last;
	} else {
	    die "what? " . substr($t, 0, 100);
	}
    }
    return %e;
}


sub definitions ($$$$) {
    my($m, $rtype, $sig, $body) = @_;
    my($template) = ($m->{"anystring"} ? "template <typename S> " : "");
    my($mname) = ($m->{"anystring"} ? $m->{"name"} . "<S>" : $m->{"name"});
    $rtype .= " " if $rtype ne "";
    $body = " " . $body if $body =~ /\A\{/;
    $body .= "\n" if $body !~ /\n\z/;
    return ("  inline $rtype$sig;\n",
	    "${template}inline $rtype${mname}::$sig$body");
}

sub define (\@$$$$) {
    my($x, $m, $rtype, $sig, $body) = @_;
    my($decl, $defn) = definitions($m, $rtype, $sig, $body);
    push @$x, $decl, $defn;
}


sub type_size_bound ($$) {
    my($ty, $na) = @_;
    if ($ty eq "S") {
	return (3, "${na}.length()");
    } elsif ($ty eq "uint32_t" || $ty eq "int32_t") {
	return 5;
    } elsif ($ty eq "uint64_t" || $ty eq "int64_t") {
	return 9;
    } else {
	# bool or enum XXX check enum bounds
	return 1;
    }
}

sub clear_body ($) {
    my($m) = @_;
    my($t) = "{\n";
    my($any_bits) = 0;
    foreach my $f (@{$m->{"f"}}) {
	next if !$f;
	my($ki, $na, $ty, $argty, $id, $ca) =
	    ($f->{"kind"}, $f->{"name"}, $f->{"type"}, $f->{"argtype"},
	     $f->{"id"}, $f->{"cast"});
	if ($ki == 0) {
	    $t .= "  ${na}_ = $ty();\n";
	} elsif ($ki == 1 && defined($f->{"bit"})) {
	    $t .= "  ${na}_ = $ty();\n";
	    $any_bits = 1;
	} elsif ($ki == 1) {
	    $t .= "  ${na}_ = $ty(" . $f->{"default"} . ");\n";
	} else {
	    $t .= "  ${na}_.clear();\n"
	}
    }
    $t .= "  _defined_ = 0;\n" if $any_bits;
    return $t . "}\n";
}


package msgpack_compact;

sub new () {
    return bless {};
}

sub unparse_type ($) {
    return 1;
}

sub size_bound_body ($$) {
    my($self, $m) = @_;
    my($t) = "";
    my(@sums) = (0);

    foreach my $f (@{$m->{"f"}}) {
	next if !$f;
	my($na, $ki) = ($f->{"name"}, $f->{"kind"});
	my($sz, $szt) = ::type_size_bound($f->{"type"},
					  $ki == 2 ? "s" : "${na}_");

	$sums[0] += ($ki == 2 ? 5 : 1);
	if (defined($szt) && $ki == 2) {
	    $t .= "  for (auto &s : ${na}_)\n    sz += $sz + $szt;\n";
	} elsif ($ki == 2) {
	    push @sums, "$sz * ${na}_.size()";
	} elsif (defined($szt)) {
	    $sums[0] += $sz;
	    push @sums, $szt;
	} else {
	    $sums[0] += $sz;
	}
    }

    if ($t ne "") {
	return "{\n  int sz = " . join(" + ", @sums) . ";\n" . $t . "  return sz;\n}\n";
    } else {
	return "{\n  return " . join(" + ", @sums) . ";\n}\n";
    }
}

sub parse_body ($$$) {
    my($self, $m, $str) = @_;
    my($t1) = "{\n  msgpack_parser p("
	. ($str ? "str" : "first")
	. ");\n";
    my($t) = "  while (p.position() < "
	. ($str ? "str.end()" : "reinterpret_cast<const char*>(last)")
	. ") {\n    switch (p.parse_tiny_int()) {\n";
    foreach my $f (@{$m->{"f"}}) {
	next if !$f;
	my($ki, $na, $ty, $argty, $id) =
	    ($f->{"kind"}, $f->{"name"}, $f->{"type"}, $f->{"argtype"},
	     $f->{"id"});
	$t .= "    case " . $id . ":\n";
	if ($f->{"cast"} ne "") {
	    $t1 .= "  int _temp_;\n" if $t1 !~ /_temp_/;
	    $t .= "      p.parse(_temp_);\n"
		. "      ${na}_ = ($ty) _temp_;\n";
	} else {
	    $t .= "      p.parse(${na}_);\n";
	}
	if (defined($f->{"bit"})) {
	    $t .= "      _defined_ |= " . $f->{"bit"} . ";\n";
	}
	$t .= "      break;\n";
    }
    return $t1 . $t . "    }\n  }\n}\n";
}

sub unparse_body ($$) {
    my($self, $m) = @_;
    my($t) = "{\n  msgpack_compact_unparser p;\n";
    foreach my $f (@{$m->{"f"}}) {
	next if !$f;
	my($ki, $na, $ty, $argty, $id, $ca) =
	    ($f->{"kind"}, $f->{"name"}, $f->{"type"}, $f->{"argtype"},
	     $f->{"id"}, $f->{"cast"});
	$ca = "($ca) " if $ca ne "";
	if ($ki == 0) {
	    $t .= "  s = p.unparse_tiny(s, $id);\n";
	    $t .= "  s = p.unparse(s, $ca${na}_);\n";
	} elsif ($ki == 1 && defined($f->{"bit"})) {
	    $t .= "  if (_defined_ & " . $f->{"bit"} . ") {\n";
	    $t .= "    s = p.unparse_tiny(s, $id);\n";
	    $t .= "    s = p.unparse(s, $ca${na}_);\n";
	    $t .= "  }\n";
	} elsif ($ki == 1) {
	    $t .= "  if (!(${na}_ == " . $f->{"default"} . ")) {\n";
	    $t .= "    s = p.unparse_tiny(s, $id);\n";
	    $t .= "    s = p.unparse(s, $ca${na}_);\n";
	    $t .= "  }\n";
	} else {
	    $t .= "  if (!${na}_.empty()) {\n";
	    $t .= "    s = p.unparse_tiny(s, $id);\n";
	    $t .= "    s = p.unparse(s, $ca${na}_);\n";
	    $t .= "  }\n";
	}
    }
    return $t . "  return s;\n}\n";
}


package msgpack_compact2;

sub new () {
    return bless {};
}

sub unparse_type ($) {
    return 2;
}

sub size_bound_body ($$) {
    my($self, $m) = @_;
    my($t) = "";
    my(@sums) = (0);

    foreach my $f (@{$m->{"f"}}) {
	next if !$f;
	my($na, $ki) = ($f->{"name"}, $f->{"kind"});
	my($sz, $szt) = ::type_size_bound($f->{"type"},
					  $ki == 2 ? "s" : "${na}_");

	$sums[0] += ($ki == 2 ? 4 : 0);
	if (defined($szt) && $ki == 2) {
	    $t .= "  for (auto &s : ${na}_)\n    sz += $sz + $szt;\n";
	} elsif ($ki == 2) {
	    push @sums, "$sz * ${na}_.size()";
	} elsif (defined($szt)) {
	    $sums[0] += $sz;
	    push @sums, $szt;
	} else {
	    $sums[0] += $sz;
	}
    }

    if ($t ne "") {
	return "{\n  int sz = " . join(" + ", @sums) . ";\n" . $t . "  return sz;\n}\n";
    } else {
	return "{\n  return " . join(" + ", @sums) . ";\n}\n";
    }
}

sub parse_body ($$$) {
    my($self, $m, $str) = @_;
    my($t) = "{\n  msgpack_parser p("
	. ($str ? "str" : "first")
	. ");\n";
    $t .= "  (void) last;\n" if !$str;
    my($temp) = undef;
    foreach my $f (@{$m->{"f"}}) {
	next if !$f;
	my($ki, $na, $ty, $argty, $rb, $in) =
	    ($f->{"kind"}, $f->{"name"}, $f->{"type"}, $f->{"argtype"}, "", "  ");
	$t .= "  int _temp_;\n" if $f->{"cast"} ne "" && $t !~ /_temp_/;
	if ($ki == 1 && defined($f->{"bit"})) {
	    $t .= "  if (!p.try_parse_null()) {\n";
	    $in .= "  ";
	}
	if ($f->{"cast"} ne "") {
	    $t .= $in . "p.parse(_temp_);\n";
	    $t .= $in . "${na}_ = ($ty) _temp_;\n";
	} else {
	    $t .= $in . "p.parse(${na}_);\n";
	}
	if (defined($f->{"bit"})) {
	    $t .= $in . "_defined_ |= " . $f->{"bit"} . ";\n";
	}
	if ($ki == 1 && defined($f->{"bit"})) {
	    $t .= "  }\n";
	}
    }
    return $t . "}\n";
}

sub unparse_body ($$) {
    my($self, $m) = @_;
    my($t) = "{\n  msgpack_compact_unparser p;\n";
    foreach my $f (@{$m->{"f"}}) {
	next if !$f;
	my($ki, $na, $ty, $argty, $id, $ca) =
	    ($f->{"kind"}, $f->{"name"}, $f->{"type"}, $f->{"argtype"},
	     $f->{"id"}, $f->{"cast"});
	$ca = "($ca) " if $ca ne "";
	if ($ki == 1 && defined($f->{"bit"})) {
	    $t .= "  if (_defined_ & " . $f->{"bit"} . ")\n";
	    $t .= "    s = p.unparse(s, $ca${na}_);\n";
	    $t .= "  else\n";
	    $t .= "    s = p.unparse_null(s);\n";
	} else {
	    $t .= "  s = p.unparse(s, $ca${na}_);\n";
	}
    }
    return $t . "  return s;\n}\n";
}


package main;

sub print_message_header ($$$) {
    my($fh, $parser, $m) = @_;
    my($mname) = $m->{"name"};
    my($template) = ($m->{"anystring"} ? "template <typename S> " : "");
    print $fh "${template}class $mname {\npublic:\n  inline $mname();\n";
    my(@defs, %bitmap);
    my($next_bit) = 1;
    my(@inits, @slots);
    my($size_simple) = 1;

    # accessor signatures
    foreach my $f (@{$m->{"f"}}) {
	next if !$f;
	my($ki, $na, $ty, $argty) = ($f->{"kind"}, $f->{"name"}, $f->{"type"},
				     $f->{"argtype"});
	if ($ki == 2) {
	    push @slots, "  std::vector<$ty> ${na}_;\n";
	    define(@defs, $m, "int", "${na}_size() const",
		   "{\n  return ${na}_.size();\n}");
	    define(@defs, $m, "void", "clear_$na()",
		   "{\n  ${na}_.clear();\n}");
	    define(@defs, $m, $argty, "$na(int i) const",
		   "{\n  return ${na}_[i];\n}");
	    define(@defs, $m, $ty . "&", "mutable_$na(int i)",
		   "{\n  return ${na}_[i];\n}");
	    define(@defs, $m, "void", "set_$na(int i, $argty x)",
		   "{\n  ${na}_[i] = x;\n}");
	    define(@defs, $m, $ty . "&", "add_$na()",
		   "{\n  ${na}_.push_back($ty());\n  return ${na}_.back();\n}");
	    define(@defs, $m, "void", "add_$na($argty x)",
		   "{\n  ${na}_.push_back(x);\n}");
	    define(@defs, $m, "const ::std::vector<$ty>&", "$na() const",
		   "{\n  return ${na}_;\n}");
	    define(@defs, $m, "::std::vector<$ty>&", "mutable_$na()",
		   "{\n  return ${na}_;\n}");
	    $size_simple = 0 if $ty eq "S";
	} else {
	    my($setbit, $clrbit, $thisbit);
	    push @slots, "  $ty ${na}_;\n";
	    if (defined($f->{"default"})) {
		push @inits, "${na}_(" . $f->{"default"} . ")";
	    } elsif ($ki == 1) {
		push @inits, "${na}_()";
	    }
	    if ($ki == 1 && !defined($f->{"default"})) {
		$f->{"bit"} = $thisbit = $next_bit;
		$next_bit <<= 1;
		define(@defs, $m, "bool", "has_$na() const",
		       "{\n  return _defined_ & $thisbit;\n}");
		($setbit, $clrbit) = ("  _defined_ |= $thisbit;\n",
				      "  _defined_ &= ~$thisbit;\n");
	    }
	    define(@defs, $m, $argty, "$na() const",
		   "{\n  return ${na}_;\n}");
	    define(@defs, $m, $ty . "&", "mutable_$na()",
		   "{\n$setbit  return ${na}_;\n}");
	    define(@defs, $m, "void", "set_$na($argty x)",
		   "{\n$setbit  ${na}_ = x;\n}");
	}
    }

    for (my $i = 0; $i < @defs; $i += 2) {
	print $fh $defs[$i];
    }
    $m->{"size_simple"} = $size_simple;
    print $fh "  void Clear();
  ", ($size_simple ? "inline " : ""), "int unparse_size_bound() const;
  uint8_t *unparse(uint8_t* x) const;
  String unparse() const;
  void parse(const uint8_t* first, const uint8_t* last);
  void parse(const String& str);
  static inline int unparse_type() {
    return ", $parser->unparse_type(), ";
  }\n";

    print $fh " private:\n";
    print $fh join("", @slots);
    print $fh "  uint32_t _defined_;\n" if $next_bit != 1;

    print $fh "};\n";

    push @inits, "_defined_(0)" if $next_bit != 1;
    my($body) = "";
    $body .= "\n  : " . join(", ", @inits) . " " if @inits;
    my($decl, $defn) = definitions($m, "", $mname . "()", $body . "{\n}");
    print $fh $defn;

    for (my $i = 1; $i < @defs; $i += 2) {
	print $fh $defs[$i];
    }

    if ($m->{"size_simple"}) {
	($decl, $defn) = definitions($m, "int", "unparse_size_bound() const", $parser->size_bound_body($m));
	print $fh $defn;
    }
}

sub print_message_body ($$$) {
    my($fh, $parser, $m) = @_;
    my($decl, $defn) = definitions($m, "void", "Clear()", clear_body($m));
    print $fh $defn;

    if (!$m->{"size_simple"}) {
	($decl, $defn) = definitions($m, "int", "unparse_size_bound() const", $parser->size_bound_body($m));
	print $fh $defn;
    }

    ($decl, $defn) = definitions($m, "uint8_t*", "unparse(uint8_t* s) const", $parser->unparse_body($m));
    print $fh $defn;

    ($decl, $defn) = definitions($m, "String", "unparse() const", "{
  StringAccum sa;
  uint8_t* s = (uint8_t*) sa.reserve(unparse_size_bound());
  uint8_t* ends = unparse(s);
  sa.adjust_length(ends - s);
  return sa.take_string();
}");
    print $fh $defn;

    ($decl, $defn) = definitions($m, "void", "parse(const uint8_t* first, const uint8_t* last)", $parser->parse_body($m, 0));
    print $fh $defn;

    ($decl, $defn) = definitions($m, "void", "parse(const String& str)", $parser->parse_body($m, 1));
    print $fh $defn;
}

sub print_header ($$\%) {
    my($fh, $parser, $e) = @_;
    print $fh "#ifndef ", uc($e->{"__pkg__"}), "_PROTOX_HH
#define ", uc($e->{"__pkg__"}), "_PROTOX_HH 1
#include \"lib/string.hh\"
#include \"lib/str.hh\"
#include \"lib/msgpack.hh\"
#include <vector>

";

    if (exists($e->{"__pkg__"})) {
	print $fh "namespace ", $e->{"__pkg__"}, " {\n";
    }

    while (($k, $v) = each(%$e)) {
	next if $k eq "__pkg__" || $v->{"type"} ne "enum";
	print $fh "enum ", $v->{"name"}, " {\n";
	$sep = "";
	foreach my $x (@{$v->{"f"}}) {
	    print $fh $sep, "  ", $x->[0], " = ", $x->[1];
	    $sep = ",\n";
	}
	print $fh "\n};\n";
	print $fh "String ", $v->{"name"}, "_Name(", $v->{"name"}, " x);\n";
    }

    while (($k, $v) = each(%$e)) {
	next if $k eq "__pkg__" || $v->{"type"} ne "message";
	print_message_header($fh, $parser, $v);
	print_message_body($fh, $parser, $v) if $v->{"anystring"};
    }

    if (exists($e->{"__pkg__"})) {
	print $fh "}\n";
    }

    print $fh "#endif\n";
}

sub print_body ($$\%) {
    my($fh, $parser, $e) = @_;
    print $fh "#include \"", $e->{"__pkg__"}, ".px.hh\"
#include \"lib/straccum.hh\"

";

    if (exists($e->{"__pkg__"})) {
	print $fh "namespace ", $e->{"__pkg__"}, " {\n";
    }


    while (($k, $v) = each(%$e)) {
	next if $k eq "__pkg__" || $v->{"type"} ne "enum";
	print $fh "String ", $v->{"name"}, "_Name(", $v->{"name"}, " x) {\n",
	"  switch (x) {\n";
	$sep = "";
	foreach my $x (@{$v->{"f"}}) {
	    print $fh "  case ", $x->[1], ": return String::make_stable(\"",
	    $x->[0], "\");\n";
	}
	print $fh "  default: return \"", $v->{"name"}, "#\" + String((int) x);\n";
	print $fh "  }\n}\n";
    }

    while (($k, $v) = each(%$e)) {
	next if $k eq "__pkg__" || $v->{"type"} ne "message" || $v->{"anystring"};
	print_message_body($fh, $parser, $v);
    }

    if (exists($e->{"__pkg__"})) {
	print $fh "}\n";
    }
}


undef $/;
if (!defined($file) || $file eq "-") {
    $t = <STDIN>;
    close STDIN;
} else {
    open(F, "<$file") || die;
    $t = <F>;
    close F;
}

my($parser) = new msgpack_compact;
my(%e) = parse_file($t);

$cpp_out = "." if !defined($cpp_out);
$cpp_out =~ s{/\z}{};
$cpp_out .= "/" . $e{"__pkg__"} . ".px.";

open(my $hfh, ">", $cpp_out . "hh") || die;
print_header($hfh, $parser, %e);
close($hfh);

open($hfh, ">", $cpp_out . "cc") || die;
print_body($hfh, $parser, %e);
close($hfh);
