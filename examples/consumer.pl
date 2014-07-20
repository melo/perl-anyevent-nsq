#!/usr/bin/env perl

use strict;
use warnings;
use lib 'lib';
use AnyEvent;
use AnyEvent::NSQ::Reader;

my ($topic, $channel) = @ARGV;
die "Usage: consumer.pl topic channel\n" unless $topic and $channel;

my $c = 1;
my $r = AnyEvent::NSQ::Reader->new(
  topic              => $topic,
  channel            => $channel,
  nsqd_tcp_addresses => '127.0.0.1',
  client_id          => "${channel}_consumer/pid_$$",

#  message_cb => sub {print STDERR "$c: $_[1]{message}\n";$c++; return},    ## return undef => mark_as_done_msg()
  message_cb => sub {return},    ## return undef => mark_as_done_msg()
);

AE::cv->recv;
