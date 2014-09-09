#!/usr/bin/env perl

use strict;
use warnings;
use FindBin;
use lib "$FindBin::Bin/../lib";
use AnyEvent;
use AnyEvent::NSQ::Reader;
use Getopt::Long;
use Time::HiRes qw( gettimeofday tv_interval );

my ($topic, $channel, $help, $verbose, $print, $stats);
GetOptions('help' => \$help, 'verbose' => \$verbose, 'print' => \$print, 'stats' => \$stats) or usage();
usage() if $help;

($topic, $channel) = @ARGV;
usage("topic and channel are required parameters") unless $topic and $channel;

my $cv = AE::cv;

my $t = my $p = 0;
my $r = AnyEvent::NSQ::Reader->new(
  topic              => $topic,
  channel            => $channel,
  nsqd_tcp_addresses => '127.0.0.1',
  client_id          => "${channel}_consumer/pid_$$",

  message_cb => \&message_handler,

  error_cb => sub { warn "$_[1]\n" if $verbose },
  disconnect_cb => sub { warn "Disconnected after $t total messages... exiting...\n" if $verbose; $cv->send },
);

if ($stats) {
  warn "Stats enabled, printed every 10 seconds\n";
  my $t0 = my $p0 = [gettimeofday];
  $stats = AE::timer 10, 10, sub {
    my $now       = [gettimeofday];
    my $elapsed_t = tv_interval($t0, $now);
    my $elapsed_p = tv_interval($p0, $now);

    my $uptime = '';
    my ($h, $m);
    if ($h = int($elapsed_t / 3600))             { $uptime .= "${h}h" }
    if ($m = int(($elapsed_t - $h * 3600) / 60)) { $uptime .= "${m}m" }
    $uptime .= int($elapsed_t - $h * 3600 - $m * 60) . 's';

    warn sprintf(
      'Stats: %0.3f mesgs/sec for the past %0.3f seconds, total messages %d, global rate %0.3f, uptime %s%s',
      $p / $elapsed_p,
      $elapsed_p, $t, $t / $elapsed_t, $uptime,"\n"
    );

    $p  = 0;
    $p0 = $now;
  };
}

my $term_sgn = AE::signal TERM => sub { $r->disconnect; undef $stats };
my $int_sgn  = AE::signal INT  => sub { $r->disconnect; undef $stats };

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

sub message_handler {
  my ($reader, $message) = @_;

  if ($print) { 
    print $message->{message}."\n";
  }

  $reader->mark_as_done_msg($message);
}
