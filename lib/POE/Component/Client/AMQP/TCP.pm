package POE::Component::Client::AMQP::TCP;

=head1 NAME

POE::Component::Client::AMQP::TCP - A modified copy of L<POE::Component::Client::TCP> to support SSL

=head1 SEE ALSO

L<POE::Component::Client::AMQP>

=head1 AUTHORS & COPYRIGHTS

POE::Component::Client::TCP is Copyright 2001-2009 by Rocco Caputo.
All rights are reserved.  POE::Component::Client::TCP is free
software, and it may be redistributed and/or modified under the same
terms as Perl itself.

POE::Component::Client::TCP is based on code, used with permission,
from Ann Barcomb E<lt>kudra@domaintje.comE<gt>.

POE::Component::Client::TCP is based on code, used with permission,
from Jos Boumans E<lt>kane@cpan.orgE<gt>.

=cut

use strict;

use vars qw($VERSION);
$VERSION = 0.01;

use Carp qw(carp croak);
use Errno qw(ETIMEDOUT ECONNRESET EAGAIN);

# Explicit use to import the parameter constants;
use POE::Session;
use POE::Driver::SysRW;
use POE::Filter::Line;
use POE::Wheel::ReadWrite;
use POE::Wheel::SocketFactory;

# Create the client.  This is just a handy way to encapsulate
# POE::Session->create().  Because the states are so small, it uses
# real inline coderefs.

sub new {
  my $type = shift;

  # Helper so we don't have to type it all day.  $mi is a name I call
  # myself.
  my $mi = $type . '->new()';

  # If they give us lemons, tell them to make their own damn
  # lemonade.
  croak "$mi requires an even number of parameters" if (@_ & 1);
  my %param = @_;

  # Validate what we're given.
  croak "$mi needs a RemoteAddress parameter"
    unless exists $param{RemoteAddress};
  croak "$mi needs a RemotePort parameter"
    unless exists $param{RemotePort};

  # Extract parameters.
  my $alias           = delete $param{Alias};
  my $address         = delete $param{RemoteAddress};
  my $port            = delete $param{RemotePort};
  my $domain          = delete $param{Domain};
  my $bind_address    = delete $param{BindAddress};
  my $bind_port       = delete $param{BindPort};
  my $ctimeout        = delete $param{ConnectTimeout};
  my $args            = delete $param{Args};
  my $session_type    = delete $param{SessionType};
  my $session_params  = delete $param{SessionParams};
  my $ssl             = delete $param{SSL};

  if ($ssl) {
    require IO::Socket::SSL;
  }

  $args = [] unless defined $args;
  croak "Args must be an array reference" unless ref($args) eq "ARRAY";

  foreach (
    qw( Connected ConnectError Disconnected ServerInput
      ServerError ServerFlushed Started
      ServerHigh ServerLow
    )
  ) {
    croak "$_ must be a coderef" if(
      defined($param{$_}) and ref($param{$_}) ne 'CODE'
    );
  }

  my $high_mark_level = delete $param{HighMark};
  my $low_mark_level  = delete $param{LowMark};
  my $high_event      = delete $param{ServerHigh};
  my $low_event       = delete $param{ServerLow};

  # this is ugly, but now its elegant :)  grep++
  my $using_watermarks = grep { defined $_ }
    ($high_mark_level, $low_mark_level, $high_event, $low_event);
  if ($using_watermarks > 0 and $using_watermarks != 4) {
    croak "If you use the Mark settings, you must define all four";
  }

  $high_event = sub { } unless defined $high_event;
  $low_event  = sub { } unless defined $low_event;

  my $conn_callback       = delete $param{Connected};
  my $conn_error_callback = delete $param{ConnectError};
  my $disc_callback       = delete $param{Disconnected};
  my $input_callback      = delete $param{ServerInput};
  my $error_callback      = delete $param{ServerError};
  my $flush_callback      = delete $param{ServerFlushed};
  my $start_callback      = delete $param{Started};
  my $filter              = delete $param{Filter};

  # Extra states.

  my $inline_states = delete $param{InlineStates};
  $inline_states = {} unless defined $inline_states;

  my $package_states = delete $param{PackageStates};
  $package_states = [] unless defined $package_states;

  my $object_states = delete $param{ObjectStates};
  $object_states = [] unless defined $object_states;

  croak "InlineStates must be a hash reference"
    unless ref($inline_states) eq 'HASH';

  croak "PackageStates must be a list or array reference"
    unless ref($package_states) eq 'ARRAY';

  croak "ObjectStates must be a list or array reference"
    unless ref($object_states) eq 'ARRAY';

  # Errors.

  croak "$mi requires a ServerInput parameter" unless defined $input_callback;

  foreach (sort keys %param) {
    carp "$mi doesn't recognize \"$_\" as a parameter";
  }

  # Defaults.

  $session_type = 'POE::Session' unless defined $session_type;
  if (defined($session_params) && ref($session_params)) {
    if (ref($session_params) ne 'ARRAY') {
      croak "SessionParams must be an array reference";
    }
  } else {
    $session_params = [ ];
  }

  $address = '127.0.0.1' unless defined $address;

  $conn_error_callback = \&_default_error unless defined $conn_error_callback;
  $error_callback      = \&_default_io_error unless defined $error_callback;

  $disc_callback  = sub {} unless defined $disc_callback;
  $conn_callback  = sub {} unless defined $conn_callback;
  $flush_callback = sub {} unless defined $flush_callback;
  $start_callback = sub {} unless defined $start_callback;

  # Spawn the session that makes the connection and then interacts
  # with what was connected to.

  return $session_type->create
    ( @$session_params,
      inline_states =>
      { _start => sub {
          my ($kernel, $heap) = @_[KERNEL, HEAP];
          $heap->{shutdown_on_error} = 1;
          $kernel->alias_set( $alias ) if defined $alias;
          $kernel->yield( 'reconnect' );
          $start_callback->(@_);
        },

        # To quiet ASSERT_STATES.
        _stop   => sub { },
        _child  => sub { },

        reconnect => sub {
          my ($kernel, $heap) = @_[KERNEL, HEAP];

          $heap->{shutdown} = 0;
          $heap->{connected} = 0;

          # Tentative patch to re-establish the alias upon reconnect.
          # Necessary because otherwise the alias goes away for good.
          # Unfortunately, there is a gap where the alias may not be
          # set, and any events dispatched then will be dropped.
          $kernel->alias_set( $alias ) if defined $alias;

          $heap->{server} = POE::Wheel::SocketFactory->new
            ( RemoteAddress => $address,
              RemotePort    => $port,
              SocketDomain  => $domain,
              BindAddress   => $bind_address,
              BindPort      => $bind_port,
              SuccessEvent  => 'got_connect_success',
              FailureEvent  => 'got_connect_error',
            );
          $_[KERNEL]->alarm_remove( delete $heap->{ctimeout_id} )
            if exists $heap->{ctimeout_id};
          $heap->{ctimeout_id} = $_[KERNEL]->alarm_set
            ( got_connect_timeout => time + $ctimeout
            ) if defined $ctimeout;
        },

        connect => sub {
          my ($new_address, $new_port) = @_[ARG0, ARG1];
          $address = $new_address if defined $new_address;
          $port    = $new_port    if defined $new_port;
          $_[KERNEL]->yield("reconnect");
        },

        got_connect_success => sub {
          my ($kernel, $heap, $socket) = @_[KERNEL, HEAP, ARG0];

          if ($ssl) {
            return unless sslify_socket(@_);
          }

          $kernel->alarm_remove( delete $heap->{ctimeout_id} )
            if exists $heap->{ctimeout_id};

          # Ok to overwrite like this as of 0.13.
          $_[HEAP]->{server} = POE::Wheel::ReadWrite->new
            ( Handle       => $socket,
              Driver       => POE::Driver::SysRW->new(),
              Filter       => _get_filter($filter),
              InputEvent   => 'got_server_input',
              ErrorEvent   => 'got_server_error',
              FlushedEvent => 'got_server_flush',
              do {
                  $using_watermarks ? return (
                    HighMark => $high_mark_level,
                    HighEvent => 'got_high',
                    LowMark => $low_mark_level,
                    LowEvent => 'got_low',
                  ) : ();
                },
            );

          $heap->{connected} = 1;
          $conn_callback->(@_);
        },
        got_high => $high_event,
        got_low => $low_event,

        got_connect_error => sub {
          my $heap = $_[HEAP];
          $_[KERNEL]->alarm_remove( delete $heap->{ctimeout_id} )
            if exists $heap->{ctimeout_id};
          $heap->{connected} = 0;
          $conn_error_callback->(@_);
          delete $heap->{server};
        },

        got_connect_timeout => sub {
          my $heap = $_[HEAP];
          $heap->{connected} = 0;
          $_[KERNEL]->alarm_remove( delete $heap->{ctimeout_id} )
            if exists $heap->{ctimeout_id};
          $! = ETIMEDOUT;
          @_[ARG0,ARG1,ARG2] = ('connect', $!+0, $!);
          $conn_error_callback->(@_);
          delete $heap->{server};
        },

        got_server_error => sub {
          $error_callback->(@_);
          if ($_[HEAP]->{shutdown_on_error}) {
            $_[KERNEL]->yield("shutdown");
            $_[HEAP]->{got_an_error} = 1;
          }
        },

        got_server_input => sub {
          my $heap = $_[HEAP];
          return if $heap->{shutdown};
          $input_callback->(@_);
        },

        got_server_flush => sub {
          my $heap = $_[HEAP];
          $flush_callback->(@_);
          if ($heap->{shutdown}) {
            delete $heap->{server};
            $disc_callback->(@_);
          }
        },

        shutdown => sub {
          my ($kernel, $heap) = @_[KERNEL, HEAP];
          $heap->{shutdown} = 1;

          $kernel->alarm_remove( delete $heap->{ctimeout_id} )
            if exists $heap->{ctimeout_id};

          if ($heap->{connected}) {
            $heap->{connected} = 0;
            if (defined $heap->{server}) {
              if (
                $heap->{got_an_error} or
                not $heap->{server}->get_driver_out_octets()
              ) {
                delete $heap->{server};
                $disc_callback->(@_);
              }
            }
          }
          else {
            delete $heap->{server};
          }

          $kernel->alias_remove($alias) if defined $alias;
        },

        # User supplied states.
        %$inline_states,
      },

      # User arguments.
      args => $args,

      # User supplied states.
      package_states => $package_states,
      object_states  => $object_states,
    )->ID;
}

sub _get_filter {
  my $filter = shift;
  if (ref $filter eq 'ARRAY') {
    my @filter_args = @$filter;
    $filter = shift @filter_args;
    return $filter->new(@filter_args);
  } elsif (ref $filter) {
    return $filter->clone();
  } elsif (!defined($filter)) {
    return POE::Filter::Line->new();
  } else {
    return $filter->new();
  }
}

# The default error handler logs to STDERR and shuts down the socket.

sub _default_error {
  unless ($_[ARG0] eq "read" and ($_[ARG1] == 0 or $_[ARG1] == ECONNRESET)) {
    warn(
      'Client ', $_[SESSION]->ID, " got $_[ARG0] error $_[ARG1] ($_[ARG2])\n"
    );
  }
  delete $_[HEAP]->{server};
}

sub _default_io_error {
  my ($syscall, $errno, $error) = @_[ARG0..ARG2];
  $error = "Normal disconnection" unless $errno;
  warn('Client ', $_[SESSION]->ID, " got $syscall error $errno ($error)\n");
  $_[KERNEL]->yield("shutdown");
}

## sslify_socket (@_)
#
#  Attempt to SSLify the passed socket.  Not to be
#  called as a state, this is a utility function that will return bool of
#  success.  It supports non-blocking SSLification so it will come back to the
#  calling state over and over again while waiting for the peer.

sub sslify_socket {
    my ($heap, $kernel, $session, $state, $socket) = @_[HEAP, KERNEL, SESSION, STATE, ARG0];
   
    # Start SSL on the socket if this hasn't yet been done
    if (ref $socket eq 'GLOB') {
        $socket = IO::Socket::SSL->start_SSL($socket, SSL_startHandshake => 0);
        # Make sure the previous operation succeeded
        if (! $socket->isa('IO::Socket::SSL')) {
            die "start_SSL did not succeed; destroying socket";
        }
        $socket->blocking(0);
    }

    #print STDERR "connect_SSL()\n";
    # Do the connect_SSL (possibly many times)
    if ($socket->connect_SSL) {
        #print STDERR $session->ID . ": client connected SSL\n";
        return 1;
    }
    elsif ($! != EAGAIN) {
        die $session->ID . ": client SSL failed";
    }
    else {
        # As a non-blocking SSL setup, we may not be ready to accept.  If that's
        # the case, give it time and come back in a minute.
        $kernel->yield($state, $socket);
        return 0;
    }
}

1;

