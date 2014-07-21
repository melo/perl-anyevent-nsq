#!/usr/bin/env perl

use strict;
use warnings;
use FindBin;
use lib "$FindBin::Bin/../lib";
use AnyEvent;
use AnyEvent::Handle;
use AnyEvent::NSQ::Writer;

my ($topic, $factor) = @ARGV;
die "Usage: publisher.pl topic [msgs_per_call]\n" unless $topic;

$factor = 1 unless $factor;
my @factors = (1 .. $factor);

my $cv = AE::cv;

## Our Publisher
my $r = AnyEvent::NSQ::Writer->new(
  nsqd_tcp_addresses => '127.0.0.1',
  client_id          => "${topic}_producer/pid_$$",

  error_cb      => sub { warn "$_[1]\n" },
  disconnect_cb => sub { warn "Got disconnected... exiting...\n"; $cv->send },
);

## Publish each input line
my $hdl;
$hdl = AnyEvent::Handle->new(fh => \*STDIN, on_error => sub { $r->disconnect; $hdl->destroy; undef $hdl });

my $c = 1;
my @readline;
@readline = (
  line => sub {
    my $line = $_[1];
    chomp($line);
    return unless length($line);

    my @messages = map {"$c.$_: $line"} @factors;

    $r->publish($topic, @messages);
    $c++;
    $_[0]->push_read(@readline);
  }
);
$hdl->push_read(@readline);

if (-t \*STDOUT) {
  print "Type lines, <enter> to publish";
  print " (multiplication factor $factor)" if $factor > 1;
  print ", type Ctrl-D to end:\n";
}

$cv->recv;
