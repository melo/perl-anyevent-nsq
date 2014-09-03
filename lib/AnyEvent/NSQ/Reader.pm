package AnyEvent::NSQ::Reader;

# ABSTRACT: a NSQ.io asynchronous consumer
# VERSION
# AUTHORITY

use strict;
use warnings;
use Carp 'croak';

use parent 'AnyEvent::NSQ::Client';

sub new {
  my $class = shift;
  my $self  = $class->SUPER::new(@_);

  # To keep registry of the connection bound to each message
  $self->{routing} = {};

  return $self;
}

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

  $self->SUPER::_identified(@_);

  $conn->subscribe(
    $self->{topic},
    $self->{channel},

    ## our message handler...
    sub {
      ## FIXME: bless it with the future AnyEvent::NSQ::Message
      my $msg = $_[1];

      ## Keep the connection in the registry in case the user
      ##  wants to issue FIN/REQs inside the callback
      $self->{routing}->{$msg->{message_id}} = $conn;

      my $action = $self->{message_cb}->($self, $msg);

      ## Action below -1 does nothing, we assume the user took care of it himself
      if (not defined $action) { $conn->mark_as_done_msg($_[1]) }
      elsif ($action >= -1) { $conn->requeue_msg($_[1], $action) }

      delete($self->{routing}->{$msg->{message_id}});
    }
  );

  $conn->ready($self->{ready_count} || int(($info->{max_rdy_count} || 2000) / 10));
}

sub mark_as_done_msg {
  my $self    = shift;
  my $message = shift;
} 

sub requeue_msg {
  my $self    = shift;
  my $message = shift;
}

sub touch_message {
  my $self    = shift;
  my $message = shift;
}

sub _find_message_connection {
  my $self    = shift;
  my $message = shift; 

  my $message_id = ref($message) ? $message->{message_id} : $message;

  return $self->{routing}->{$message_id};
}

1;
