#!/usr/bin/env perl

use strict;
use warnings;
use FindBin;
use lib "$FindBin::Bin/../lib";
use AnyEvent;
use AnyEvent::NSQ::Lookupd;
use Getopt::Long;

my ($help, $verbose, $interval);
GetOptions('help' => \$help, 'verbose' => \$verbose, 'interval=i' => \$interval) or usage();
usage() if $help;

my ($topic, @nsqlookupds) = @ARGV;
usage("topic is a required parameter") unless $topic and length($topic);
usage("at least one lookupd address is required") unless @nsqlookupds;

my $cv = AE::cv;

my $l = AnyEvent::NSQ::Lookupd->new(
  topic              => $topic,
  pooling_interval => $interval,
  
  lookupd_http_addresses => \@nsqlookupds,

  add_nsqd_cb  => sub { print "$topic: added $_[0], version $_[1]{version}\n" },
  drop_nsqd_cb => sub { print "$topic: dropped $_[0], version $_[1]{version}\n" },
);

$cv->recv;


sub usage {
  print "Error: @_\n" if @_;

  print <<"  EOU";
Usage: lookupd_monitor.pl [options] topic lookup_addresses...

  Monitors a set of nsqlookupd's and lists all nsqd's that produce a particular topic.

  Options:

  --help or -h               Prints this message and exits

  --interval=INT or -i=INT   Sets the polling interval
  EOU

  exit(1);
}
