package AnyEvent::NSQ::Lookupd;

# ABSTRACT: nsqlookupd pooler
# VERSION
# AUTHORITY

use strict;
use warnings;
use AnyEvent;
use AnyEvent::HTTP;

sub new {
  my ($class, %args) = @_;
  my $self = bless { producers => {}, sources => {}, pooling_interval => 60 }, $class;

  for my $p (qw( topic lookupd_http_addresses add_nsqd_cb drop_nsqd_cb )) {
    next unless exists $args{$p} and defined $args{$p};
    $self->{$p} = delete $args{$p};
    croak(qq{FATAL: parameter '$p' must be a CodeRef}) if $p =~ m{_cb$} and ref($self->{$p}) ne 'CODE';
  }

  $self->{pooling_interval} = delete($args{pooling_interval})
    if exists $args{pooling_interval} and $args{pooling_interval} > 0;

  $self->{lookupd_http_addresses} = [$self->{lookupd_http_addresses}]
    unless ref($self->{lookupd_http_addresses}) eq 'ARRAY';

  $self->_start_poller;

  return $self;
}

sub poll {
  my ($self) = @_;
  my $urls = $self->_get_polling_urls;

  for my $url (@$urls) {
    http_get $url, sub { $self->_parse_lookup_response($url, $_[0]) if defined $_[0] };
  }

  return;
}

sub start {
  my ($self);

  $self->{poller_timer} = AE::timer 0, $self->{pooling_interval}, sub { $self->poll() };
}

sub stop { delete $_[0]->{poller_timer} }

sub is_running { return exists $_[0]->{poller_timer} }


#### The actual work...

sub _get_polling_urls {
  my ($self) = @_;

  my @urls;
  for my $address (@{ $self->{lookupd_http_addresses} }) {
    $address = "http://$address" unless $address =~ m/^https?:/;
    push @urls, "$address/lookup?topic=$self->{topic}";
  }

  return \@urls;
}

sub _parse_lookup_response {
  my ($self, $source, $body) = @_;

  my $json = eval { JSON::XS::decode_json($body) };
  return unless $json;
  return unless $json->{status_code} and $json->{status_code} == 200;

  $self->_update_nsqd_list_for_source($source, $json->{data});
}

sub _update_nsqd_list_for_source {
  my ($self, $source, $data) = @_;
  my $source_data = $self->{sources}{$source} = {};

  for my $producer (@{ $data->{producers} || [] }) {
    my $bcast_addr = $producer->{broadcast_address};
    my $tcp_port   = $producer->{tcp_port};
    next unless $bcast_addr and $tcp_port;

    my $nsqd_id = "$bcast_addr:$tcp_port";

    $source_data->{$nsqd_id} = {
      nsqd_id => $nsqd_id,
      info    => $producer,
    };
  }

  $self->_merge_sources();
}

sub _merge_sources {
  my ($self) = @_;

  my %merged_producers;
  for my $source_producers (values %{ $self->{sources} }) {
    for my $producer (values %$source_producers) {
      $merged_producers{ $producer->{nsqd_id} } = $producer;
    }
  }

  $self->_detect_changes_producers(\%merged_producers);
}

sub _detect_changes_producers {
  my ($self, $merged_producers) = @_;
  my $producers = $self->{producers};

  ## detect dropped nsqd's
  for my $nsqd_id (keys %$producers) {
    next if exists $merged_producers->{$nsqd_id};
    $self->{drop_nsqd_cb}->($self, $nsqd_id, delete $producers->{$nsqd_id});
  }

  ## add the new ones
  for my $nsqd_id (keys %$merged_producers) {
    next if exists $producers->{$nsqd_id}
    $self->{add_nsqd_cb}->($self, $nsqd_id, $producers->{$nsqd_id} = $merged_producers->{$nsqd_id});
  }
}

1;
