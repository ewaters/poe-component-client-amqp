package POE::Component::Client::AMQP::Channel;

=head1 NAME

POE::Component::Client::AMQP::Channel - AMQP Channel object

=head1 DESCRIPTION

Create using the L<POE::Component::Client::AMQP> objects' C<channel()> command.  Calling C<channel()> also retrieves previously created Channel objects.  Alternatively, you can use the C<create()> class method below to create it directly.

=cut

use strict;
use warnings;
use POE;
use Params::Validate;
use Carp;
use POE::Component::Client::AMQP qw(:constants);
use base qw(Class::Accessor);
__PACKAGE__->mk_accessors(qw(id server Alias));

our $VERSION = 0.01;

=head1 CLASS METHODS 

=head2 create (...)

=over 4

Creates a new L<POE::Session> that controls communications on a new AMQP channel.  Pass a list of key/value pairs as follows:

=over 4

=item I<id> (optional)

Provide an id or one will be generated for you.

=item I<server> (required)

Pass the L<POE::Component::Client::AMQP> parent object here.

=item I<Alias> (default: ${parent_alias}-channel-$id)

The L<POE::Session> alias so you can post to it's POE states.

=item I<Callbacks> (default: {})

At the moment, only 'Created' is used.

=item I<CascadeFailure> (default: 1)

If this channel is closed, close also the server connection.

=back

Returns an object in this class.

=back

=cut

my $_ids = 0;

sub create {
    my $class = shift;

    my %args = validate(@_, {
        id     => 0,
        server => 1,

        # User definable
        Alias     => 0,
        Callbacks => { default => {} },
        CascadeFailure => { default => 1 },
        CloseCallback => { default => sub {} },
        
        # Private
        consumers => { default => {} },
        is_created => { default => 0 },
    });

    # Ensure we have a unique channel id

    my $ids = $args{server}{channels};

    if ($args{id} && $ids->{ $args{id} }) {
        croak "Channel id $args{id} is already in use";
    }
    elsif (! $args{id}) {
        foreach my $i (1 .. (2 ** 16 - 1)) {
            if (! $ids->{$i}) {
                $args{id} = $i;
                last;
            }
        }
        croak "Ran out of channel ids (!!)" unless $args{id};
    }

    # Ensure we have a unique alias name

    $args{Alias} ||= $args{server}{Alias} . '-channel-' . $_ids++;

    # Create the object and session

    my $self = bless \%args, $class;

    POE::Session->create(
        object_states => [
            $self => [qw(
                _start
                server_input
                server_send
                channel_created
            )],
        ],
    );

    # Store and return

    $ids->{ $self->{id} } = $self;
    
    return $self;
}

## Public Object Methods ###

=head1 OBJECT METHODS

=head2 id

=over 4

Returns the channel id.

=back

=head2 server

=over 4

Returns the L<POE::Component::Client::AMQP> parent object.

=back

=head2 Alias

=over 4

Returns the L<POE::Session> alias of the controlling session.

=back

=head2 do_when_created (...)

=over 4

See L<POE::Component::Client::AMQP::do_when_startup()>; similar behavior.

=back

=cut

sub do_when_created {
    my ($self, $subref) = @_;

    if ($self->{is_created}) {
        $subref->();
    }
    else {
        push @{ $self->{Callbacks}{Created} }, $subref;
    }
}

=head2 send_frames (...)

=over 4

Same as the POE state L<server_send>, but can be called on the object and before the channel is created.

=back

=cut

sub send_frames {
    my ($self, @frames) = @_;

    $self->do_when_created(sub {
        $poe_kernel->post($self->{Alias}, server_send => @frames);
    });
}

### Deferred methods ###

=head2 queue ($name, \%opts)

=over 4

Creates a new queue named $name.  If no name is given, the server will generate a name for you, which will be available in $queue_object->name after the queue is created.  Returns a new L<POE::Component::Client::AMQP::Queue> object.  This is a deferred call, similar to L<POE::Component::Client::AMQP::channel()>, so it can be used immediately.

If you pass %opts, the values you pass will override defaults in the L<Net::AMQP::Protocol::Queue::Declare::new()> call.  These are arguments like 'ticket', 'passive', 'durable', 'exclusive', 'auto_delete', 'nowait' and 'arguments' (for version 0-8 of the protocol).  See L<Net::AMQP::Protocol::Queue::Declare> for the details of these arguments.

=back

=cut

sub queue {
    my ($self, $name, $user_opts) = @_;
    $user_opts ||= {};

    if (defined $name && $self->{queues}{$name}) {
        return $self->{queues}{$name};
    }

    # Queue doesn't exist; create the object and setup deferred creation triggers

    my %opts = (
        ticket      => 0,
        queue       => (defined $name ? $name : ''),
        #passive     => 0, # if set, server will not create the queue; checks for existance
        #durable     => 0, # will remain active after restart
        exclusive   => 1, # may only be consumed from the current connection
        auto_delete => 1, # queue is deleted after the last consumer
        #nowait      => 0, # do not send a DeclareOk response
        #arguments   => {},
        %$user_opts,
    );

    # TODO: if user sets $opts{nowait}, we can't do the synchronous_callback below

    my $queue = POE::Component::Client::AMQP::Queue->create(
        name => $name,
        channel => $self,
        %opts,
    );

    # Remember it here if we have a name; otherwise wait for the callback
    $self->{queues}{$name} = $queue if defined $name;

    $self->do_when_created(sub {
        $poe_kernel->post($self->{Alias}, server_send => 
            Net::AMQP::Frame::Method->new(
                synchronous_callback => sub {
                    if (! defined $name) {
                        # I didn't know the name of the queue at the time of Queue.Declare
                        my $response_frame = $_[0]->method_frame;
                        $self->{queues}{ $response_frame->queue } = $queue;
                        $queue->name( $response_frame->queue );
                    }
                    $queue->created()
                },
                method_frame => Net::AMQP::Protocol::Queue::Declare->new(%opts),
            ),
        );
    });

    return $queue;
}

sub close {
    my $self = shift;

    $self->do_when_created(sub {
        $poe_kernel->post($self->{Alias}, server_send => 
            Net::AMQP::Protocol::Channel::Close->new()
        );
    });
}

=head1 POE STATES

The following are states you can post to to interact with the client.  Use the alias defined in the C<create()> call above.

=cut

sub _start {
    my ($self, $kernel) = @_[OBJECT, KERNEL];

    $kernel->alias_set($self->{Alias});

    # Request the AMQP creation of the channel

    $self->{server}->do_when_startup(sub { 
        $kernel->post($self->{Alias}, server_send =>
            Net::AMQP::Frame::Method->new(
                synchronous_callback => sub {
                    $kernel->post($self->{Alias}, 'channel_created');
                },
                method_frame => Net::AMQP::Protocol::Channel::Open->new(),
            ),
        );
    });
}

sub channel_created {
    my ($self, $kernel) = @_[OBJECT, KERNEL];

    # Call the callbacks if present
    if ($self->{Callbacks}{Created}) {
        foreach my $subref (@{ $self->{Callbacks}{Created} }) {
            $subref->();
        }
    }

    $self->{is_created} = 1;
}

sub server_input {
    my ($self, $kernel, $frame) = @_[OBJECT, KERNEL, ARG0];

    if ($self->{should_be_dead}) {
        $self->server->{Logger}->error("!! I should be dead !!");
    }

    if ($frame->isa('Net::AMQP::Frame::Method')) {
        my $method_frame = $frame->method_frame;

        # TODO: There are probably other methods that have content following them
        if ($method_frame->isa('Net::AMQP::Protocol::Basic::Deliver')
            || $method_frame->isa('Net::AMQP::Protocol::Basic::Return')) {
            # Start collecting content
            $self->{collecting_content} = { method_frame => $method_frame };
            return;
        }
        elsif ($self->{collecting_content}) {
            $self->server->{Logger}->error("Channel ".$self->id." got method call $method_frame when content (header or body) was expected");
            return;
        }
        elsif ($method_frame->isa('Net::AMQP::Protocol::Channel::Close')) {
            # Come up with a descriptive reason why the channel closed

            my $close_reason;
            # If the Channel.Close event gives class and method id indicating what event cause the closure, find a
            # friendly name from this and use it in the close reason
            if ($method_frame->class_id && $method_frame->method_id) {
                my $method_class = Net::AMQP::Frame::Method->registered_method_class($method_frame->class_id, $method_frame->method_id);
                my ($class_name, $method_name) = $method_class =~ m{^Net::AMQP::Protocol::(.+)::(.+)$};
                $close_reason = "The method $class_name.$method_name caused channel " . $self->id . ' to be';
            }
            else {
                $close_reason = "The channel has been";
            }
            $close_reason .= ' closed by the server: ' . $method_frame->reply_code . ': ' . $method_frame->reply_text;

            $self->server->{Logger}->error($close_reason);

            if ($self->{CloseCallback}) {
                $self->{CloseCallback}->($close_reason);
            }

            if ($self->{CascadeFailure}) {
                $self->server->stop()
            }
            else {
                $kernel->call($self->{Alias}, 'server_send',
                    Net::AMQP::Protocol::Channel::CloseOk->new()
                );
            }

            # Delete references to myself in the server
            delete $self->server->{channels}{ $self->id };
            delete $self->server->{wait_synchronous}{ $self->id };

            # Delete the reference to the server to reduce circular references
            #$self->{server} = undef;;
            $self->{should_be_dead} = 1;

            $kernel->alias_remove( $self->{Alias} );

            return;
        }
        elsif ($method_frame->isa('Net::AMQP::Protocol::Channel::CloseOk')) {
            if ($self->{CloseOkCallback}) {
                $self->{CloseOkCallback}->();
            }

            # Delete references to myself in the server
            delete $self->server->{channels}{ $self->id };
            delete $self->server->{wait_synchronous}{ $self->id };

            $self->server->{Logger}->info("Closing channel ".$self->id."; now have channels " . join(', ', sort { $a <=> $b } keys %{ $self->server->{channels} }) . " open");

            # Delete the reference to the server to reduce circular references
            #$self->{server} = undef;;
            $self->{should_be_dead} = 1;

            $kernel->alias_remove( $self->{Alias} );

            return;
        }

    }
    elsif ($frame->isa('Net::AMQP::Frame::Header')) {
        my $header_frame = $frame->header_frame;

        my $content_meta = $self->{collecting_content};
        if (! $content_meta) {
            $self->server->{Logger}->error("Channel ".$self->id." got header frame $header_frame when not expecting it");
            return;
        }

        $content_meta->{header_frame} = $header_frame;
        $content_meta->{$_} = $frame->$_ foreach qw(weight body_size);
    }
    elsif ($frame->isa('Net::AMQP::Frame::Body')) {
        my $content_meta = $self->{collecting_content};
        if (! $content_meta) {
            $self->server->{Logger}->error("Channel ".$self->id." got body frame when not expecting it");
            return;
        }

        push @{ $content_meta->{body_frames} }, $frame;
        $content_meta->{payload} .= $frame->payload;
        $content_meta->{body_size_received} += length $frame->payload;
        if ($content_meta->{body_size_received} == $content_meta->{body_size}) {
            # Done collecting content
            delete $self->{collecting_content};

            if ($content_meta->{method_frame}->isa('Net::AMQP::Protocol::Basic::Return')) {
                # FIXME
                $self->server->{Logger}->error("Channel ".$self->id." received a returned payload (".$content_meta->{method_frame}->reply_code . ': ' . $content_meta->{method_frame}->reply_text ."); no way yet to handle this");
                return;
            }

            my $consumer_tag = $content_meta->{method_frame}->consumer_tag;
            my $consumer_data = $self->{consumers}{$consumer_tag};
            if (! $consumer_data) {
                $self->server->{Logger}->error("Channel ".$self->id." received content with consumer tag $consumer_tag"
                    . " when no record of this Consume subscription exists");
                return;
            }

            $content_meta->{$_} = $consumer_data->{$_} foreach qw(queue opts);

            # Let the consumer know via the recorded callback
            my $callback_return = $consumer_data->{callback}($content_meta->{payload}, $content_meta);

            # The return value is normally ignored unless the Consume call had 'no_ack => 0',
            # in which case a 'true' response from the callback will automatically ack
            if (defined $callback_return && ! $consumer_data->{opts}{no_ack}) {
                my @message;
                if ($callback_return eq AMQP_ACK) {
                    push @message, Net::AMQP::Protocol::Basic::Ack->new(
                        delivery_tag => $content_meta->{method_frame}->delivery_tag
                    );
                }
                elsif ($callback_return eq AMQP_REJECT) {
                    push @message, Net::AMQP::Protocol::Basic::Reject->new(
                        delivery_tag => $content_meta->{method_frame}->delivery_tag,
                        requeue => 1,
                    );
                }
                $kernel->call($self->{Alias}, server_send => @message) if @message;
            }
        }
    }
}

=head2 server_send (@frames)

=over 4

Wraps the parent L<POE::Component::Client::AMQP::server_send()> method, setting the channel id of all the frames sent to my channel id.

=back

=cut

sub server_send {
    my ($self, $kernel, @output) = @_[OBJECT, KERNEL, ARG0 .. $#_];

    # Override the channel on each frame with my own channel id
    my @frames;
    foreach my $output (@output) {
        if (! ref $output) {
            print STDERR "Invalid output value '$output' to channel server_send\n";
            next;
        }

        my $frame = $output->isa("Net::AMQP::Protocol::Base") ? $output->frame_wrap : $output;
        $frame->channel($self->id);
        push @frames, $frame;
    }

    # Pass through to the server session
    $kernel->post($self->{server}{Alias}, server_send => @frames);
}

=head1 SEE ALSO

L<POE::Component::Client::AMQP>

=head1 COPYRIGHT

Copyright (c) 2009 Eric Waters and XMission LLC (http://www.xmission.com/).  All rights reserved.  This program is free software; you can redistribute it and/or modify it under the same terms as Perl itself.

The full text of the license can be found in the LICENSE file included with this module.

=head1 AUTHOR

Eric Waters <ewaters@gmail.com>

=cut

1;
