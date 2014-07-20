package AnyEvent::NSQ::Reader;

# ABSTRACT: a NSQ.io asynchronous consumer
# VERSION
# AUTHORITY

use strict;
use warnings;
use Carp 'croak';

use parent 'AnyEvent::NSQ::Client';


#### Parameter parsing

## Parse all Reader specific arguments to new()
sub _parse_args {
  my ($self, $args) = @_;

  $self->{topic}      = delete $args->{topic}      or croak q{FATAL: required 'topic' parameter is missing};
  $self->{channel}    = delete $args->{channel}    or croak q{FATAL: required 'channel' parameter is missing};
  $self->{message_cb} = delete $args->{message_cb} or croak q{FATAL: required 'message_cb' parameter is missing};

  for my $arg (qw( ready_count requeue_delay )) {
    $self->{$arg} = delete($args->{$arg}) if exists $args->{$arg};
  }

  return $self->SUPER::_parse_args($args);
}


### Connection setup and teardown

## called after a successful reply to IDENTIFY, we are ready to rock & roll
sub _identified {
  my ($self, $conn, $info) = @_;

  $conn->subscribe(
    $self->{topic},
    $self->{channel},

    ## our message handler...
    sub {
      ## FIXME: bless it with the future AnyEvent::NSQ::Message
      my $msg = $_[1];

      my $action = $self->{message_cb}->($self, $msg);

      ## Action below -1 does nothing, we assume the user took care of it himself
      if (not defined $action) { $conn->mark_as_done_msg($_[1]) }
      elsif ($action >= -1) { $conn->requeue_msg($_[1], $action) }
    }
  );

  $conn->ready($self->{ready_count} || int(($info->{max_rdy_count} || 2000) / 10));
}

1;
