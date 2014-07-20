#!/usr/bin/env perl

use strict;
use warnings;
use HTTP::Tiny;

my ($topic, $factor, $limit) = @ARGV;
die "Usage: http_publisher.pl topic [msgs_per_call]\n" unless $topic;

$factor = 1 unless $factor;

my $ua = HTTP::Tiny->new(keep_alive => 1);
my $url = "http://127.0.0.1:4151/mput?topic=$topic";

my $c       = 1;
my @factors = (1 .. $factor);
my $body;
while (1) {
  $body = join("\n", map {"Request $c.$_"} @factors);

  my $res = $ua->post($url, { content => $body });
  use DDP;
  p($res) unless $res->{success};

  $c++;
  last if defined($limit) and --$limit == 0;
}
