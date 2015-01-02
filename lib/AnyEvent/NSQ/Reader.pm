package AnyEvent::NSQ::Reader;

# ABSTRACT: a NSQ.io asynchronous consumer
# VERSION
# AUTHORITY

use strict;
use warnings;
use Carp 'croak';
use Scalar::Util qw( weaken );

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
      weaken( $self->{routing}->{$msg->{message_id}} );

      $self->{message_cb}->($self, $msg);
    }
  );

  $conn->ready($self->{ready_count} || int(($info->{max_rdy_count} || 2000) / 10));
}

sub mark_as_done_msg {
  my ($self, $msg) = @_;

  my $conn = $self->_find_and_delete_message_connection($msg);

  $conn->mark_as_done_msg($msg);
  return 1;
} 

sub requeue_msg {
  my ($self, $msg, $delay) = @_;

  my $conn = $self->_find_and_delete_message_connection($msg);

  $conn->requeue_msg($msg, $delay);
  return 1;
}

sub touch_msg {
  my ($self, $msg) = @_;

  my $conn = $self->_find_and_delete_message_connection($msg);
  
  $conn->touch_msg($msg);
  return 1;
}
*touch_message = \&touch_msg;

sub _find_and_delete_message_connection {
  my ($self, $msg) = @_;

  my $id = ref($msg) ? $msg->{message_id} : $msg;

  my $conn = delete($self->{routing}->{$id});
 
  if ( !$conn ) {
    croak "WARN: Could not find the connection to route msg $id";
  }

  return $conn;
}

1;
