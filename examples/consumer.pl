#!/usr/bin/env perl

use strict;
use warnings;
use FindBin;
use lib "$FindBin::Bin/../lib";
use AnyEvent;
use AnyEvent::NSQ::Reader;
use Getopt::Long;

my ($topic, $channel, $help, $verbose, $print);
GetOptions('help' => \$help, 'verbose' => \$verbose, 'print' => \$print) or usage();
usage() if $help;

($topic, $channel) = @ARGV;
usage("topic and channel are required parameters") unless $topic and $channel;

my $cv = AE::cv;

## return undef => mark_as_done_msg()
my $message_cb = $print ? sub { print "$_[1]{message}\n"; return } : sub {return};

my $t = 0;
my $r = AnyEvent::NSQ::Reader->new(
  topic              => $topic,
  channel            => $channel,
  nsqd_tcp_addresses => '127.0.0.1',
  client_id          => "${channel}_consumer/pid_$$",

  message_cb => sub { $t++; $message_cb->(@_, $t) },

  error_cb => sub { warn "$_[1]\n" if $verbose },
  disconnect_cb => sub { warn "Disconnected after $t total messages... exiting...\n" if $verbose; $cv->send },
);

my $term_sgn = AE::signal TERM => sub { $r->disconnect };
my $int_sgn  = AE::signal INT  => sub { $r->disconnect };

$cv->recv;


sub usage {
  print "Error: @_\n" if @_;

  print <<"  EOU";
Usage: consumer.pl [--help|-h] [--print|-p] topic channel

  Consume messages from channel <channel> linked to topic <topic>.
  Topic and channel parameters are mandatory.

  Options:

  --help or -h     Prints this message and exits
  --verbose or -v  Writes debug information about connections,
                   disconnections and errors

  --print or -p    Prints each received message
  EOU

  exit(1);
}
