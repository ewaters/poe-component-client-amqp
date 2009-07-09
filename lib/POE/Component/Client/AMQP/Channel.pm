package POE::Component::Client::AMQP::Channel;

use strict;
use warnings;
use POE;
use Params::Validate;
use Data::Dumper;
use Carp;
use base qw(Class::Accessor);
__PACKAGE__->mk_accessors(qw(id server));

# FIXME: this should be a server-specific global, not class-wide
my @_ids;

## Class Methods ###

sub create {
    my $class = shift;

    my %args = validate(@_, {
        id     => 0,
        server => 1,

        # User definable
        Alias     => 0,
        Callbacks => { default => {} },
    });

    # Ensure we have a unique channel id

    if ($args{id} && $_ids[ $args{id} ]) {
        croak "Channel id $args{id} is already in use";
    }
    elsif (! $args{id}) {
        foreach my $i (1 .. (2 ** 16 - 1)) {
            if (! $_ids[$i]) {
                $args{id} = $i;
                last;
            }
        }
        croak "Ran out of channel ids (!!)" unless $args{id};
    }

    # Ensure we have a unique alias name

    $args{Alias} ||= $args{server}{Alias} . '-channel-' . $args{id};

    # Create the object and session

    my $self = bless \%args, $class;

    POE::Session->create(
        object_states => [
            $self => [qw(
                _start
                server_input
                server_send
                channel_opened
            )],
        ],
    );

    # Store and return

    $_ids[ $self->{id} ] = $self;
    
    return $self;
}

sub find_or_create {
    my ($class, $id, $server) = @_;

    if ($_ids[$id]) {
        return $_ids[$id];
    }
    else {
        return $class->create(id => $id, server => $server);
    }
}

## Public Object Methods ###

sub on_startup {
    my $self = shift;

    push @{ $self->{Callbacks}{Startup} }, @_;
}

sub add_periodic_timer {
}

sub queue {
    my $self = shift;

    my $name = shift; # FIXME

    # TODO - move to AMQP.pm
    if (! $self->{queues}{$name}) {
        my $queue = POE::Component::Client::AMQP::Queue->create(
            name => $name,
            channel => $self,
        );

        $poe_kernel->post($self->{Alias}, server_send => 
            {
                synchronous_callback => sub {
                    $queue->created()
                },
            },
            Net::AMQP::Protocol::Queue::Declare->new(
                ticket      => 0,
                queue       => $name,
                passive     => 0, # if set, server will not create the queue; checks for existance
                durable     => 0, # will remain active after restart
                exclusive   => 1, # may only be consumed from the current connection
                auto_delete => 1, # queue is deleted after the last consumer
                nowait      => 0, # do not send a DeclareOk response
                arguments   => {},
            )
        );
        
        $self->{queues}{$name} = $queue;
    }

    return $self->{queues}{$name};
}

## Private Object Methods ###



## POE Methods ###

sub _start {
    my ($self, $kernel) = @_[OBJECT, KERNEL];

    $kernel->alias_set($self->{Alias});
}

sub server_input {
    my ($self, $kernel, $input_parsed) = @_[OBJECT, KERNEL, ARG0];

    if (my $frame = $input_parsed->{method_frame}) {
        if ($frame->isa('Net::AMQP::Protocol::Channel::OpenOk')) {
            $kernel->yield('channel_opened');
        }
    }
}

sub server_send {
    my ($self, $kernel, @output) = @_[OBJECT, KERNEL, ARG0 .. $#_];

    #print STDERR "Server_send: " . Dumper(\@output);

    my $opts = {};
    if (defined $output[0] && ref $output[0] && ref $output[0] eq 'HASH') {
        $opts = shift @output;
    }

    $opts->{channel} = $self->id;

    # Pass through to the server session
    $kernel->post($self->{server}{Alias}, server_send => $opts, @output);
}

sub channel_opened {
    my ($self, $kernel) = @_[OBJECT, KERNEL];

    # Call the callbacks if present
    if ($self->{Callbacks}{Startup}) {
        foreach my $subref (@{ $self->{Callbacks}{Startup} }) {
            $subref->();
        }
    }
}

1;
