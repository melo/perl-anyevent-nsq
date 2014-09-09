#!perl

use strict;
use warnings;
use Test::More;
use AnyEvent;
use AnyEvent::NSQ::Reader;

subtest 'basic connection' => sub {
  my $r = AnyEvent::NSQ::Reader->new(
    topic              => 'test',
    channel            => 'anyevent-nsq-reader-test',
    nsqd_tcp_addresses => '127.0.0.1',
    client_id          => "10-reader.t/$$",

    ready_count => 5,

    message_cb => sub {
      print STDERR "!!!! GOT MESSAGE '$_[1]{message}\n";
      $_[0]->mark_as_done_msg($_[1]);
      return;
    },
  );

  AE::cv->recv;
};


done_testing();
