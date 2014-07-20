package AnyEvent::NSQ::Reader;

# ABSTRACT: a NSQ.io asynchronous consumer
# VERSION
# AUTHORITY

use strict;
use warnings;
use AnyEvent;
use AnyEvent::Socket ();
use Carp 'croak';
use AnyEvent::NSQ::Connection;

sub new {
  my ($class, %args) = @_;
  my $self = bless {}, $class;

  $self->{topic}      = delete $args{topic}       or croak q{FATAL: required 'topic' parameter is missing};
  $self->{channel}    = delete $args{channel}     or croak q{FATAL: required 'channel' parameter is missing};
  $self->{message_cb} = delete($args{message_cb}) or croak q{FATAL: required 'message_cb' parameter is missing};

  $self->{disconnect_cb} = delete($args{disconnect_cb}) || sub { };
  $self->{error_cb}      = delete($args{error_cb})      || sub { croak($_[1]) };

  for my $arg (qw( ready_count client_id hostname requeue_delay )) {
    $self->{$arg} = delete($args{$arg}) if exists $args{$arg};
  }

  if (my $lookupd_http_addresses = delete $args{lookupd_http_addresses}) {
    $lookupd_http_addresses = [$lookupd_http_addresses] unless ref($lookupd_http_addresses) eq 'ARRAY';
    $self->{lookupd_http_addresses} = $lookupd_http_addresses;
    $self->{use_lookupd}            = 1;
  }

  if (my $nsqd_tcp_addresses = delete $args{nsqd_tcp_addresses}) {
    croak(q{FATAL: only one of 'lookupd_http_addresses' and 'nsqd_tcp_addresses' is allowed}) if $self->{use_lookupd};

    $nsqd_tcp_addresses = [$nsqd_tcp_addresses] unless ref($nsqd_tcp_addresses) eq 'ARRAY';
    $self->{nsqd_tcp_addresses} = $nsqd_tcp_addresses;
    $self->{use_lookupd}        = 0;
  }

  ## There can be only one, there must be at least one
  croak(q{FATAL: one of 'nsqd_tcp_addresses' or 'lookup'}) unless defined $self->{use_lookupd};

  $self->connect();

  return $self;
}

sub connect {
  my $self = shift;

  if ($self->{use_lookupd}) {
    $self->_start_lookupd_poolers;
  }
  else {
    $self->_start_nsqd_connections;
  }

  return;
}

sub _start_nsqd_connections {
  my ($self) = @_;

  for my $nsqd_tcp_address (@{ $self->{nsqd_tcp_addresses} }) {
    $self->_start_nsqd_connection($nsqd_tcp_address, reconnect => 1);
  }
}

sub _start_nsqd_connection {
  my ($self, $nsqd_tcp_address, %args) = @_;

  my $conns = $self->{nsqd_conns} ||= {};
  return if $conns->{$nsqd_tcp_address};

  my ($host, $port) = AnyEvent::Socket::parse_hostport($nsqd_tcp_address, 4150);    ## 4150 is the default port for nsqd
  croak(qq{FATAL: could not parse '$nsqd_tcp_address' as a valid address/port combination}) unless $host and $port;

  my %conn = (host => $host, port => $port);
  for my $arg (qw( client_id hostname error_cb requeue_delay )) {
    $conn{$arg} = $self->{$arg} if exists $self->{$arg};
  }

  $conn{connect_cb} = sub {
    my ($conn) = @_;
    $conn->identify(
      sub {
        $conn->subscribe(
          $self->{topic},
          $self->{channel},
          sub {
            ## FIXME: bless it with the future AnyEvent::NSQ::Message
            my $msg = $_[1];

            my $action = $self->{message_cb}->($self, $msg);

            ## Action below -1 does nothing, we assume the user took care of it himself
            if (not defined $action) { $conn->mark_as_done_msg($_[1]) }
            elsif ($action >= -1) { $conn->requeue_msg($_[1], $action) }
          }
        );

        $conn->ready($self->{ready_count} || int(($_[1]{max_rdy_count} || 2000) / 10));
      }
    );
  };

  $conn{disconnect_cb} = sub {
    ## FIXME: deal with reconnects here
    $self->{disconnect_cb}->(@_) if $self->{disconnect_cb};
  };

  $conns->{$nsqd_tcp_address}{conn}  = AnyEvent::NSQ::Connection->new(%conn);
  $conns->{$nsqd_tcp_address}{state} = 'connecting';

  return;
}

sub _start_lookupd_poolers { }

1;
