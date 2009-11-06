package POE::Component::Client::AMQP::Queue;

=head1 NAME

POE::Component::Client::AMQP::Queue - AMQP Queue object

=head1 DESCRIPTION

Create using the L<POE::Component::Client::AMQP::Channel> objects' C<queue()> command.  Calling C<queue()> also retrieves previously created Queue objects.  Alternatively, you can use the C<create()> class method below to create it directly, but doing so will not send the Queue.Declare call to the AMQP server.

=cut

use strict;
use warnings;
use POE;
use Params::Validate qw(validate_with);

use base qw(Class::Accessor);
__PACKAGE__->mk_accessors(qw(name channel is_created));

our $VERSION = 0.01;

=head1 CLASS METHODS

=head2 create (...)

=over 4

Pass two named args 'name' (optional) and 'channel'.

=back

=cut

sub create {
    my $class = shift;

    my %self = validate_with(
        params => \@_,
        spec => {
            name    => 0,
            channel => 1,

            is_created => { default => 0 },
            on_created => { default => [] },
        },
        allow_extra => 1,
    );

    return bless \%self, $class;
}

=head1 OBJECT METHODS

=head2 name

=over 4

Returns the queue name

=back

=head2 channel

=over 4

Returns the L<POE::Component::Client::AMQP::Channel> parent object.

=back

=head2 is_created

=over 4

Returns a boolean, indicating wether the queue has been created on the AMQP server yet or not.

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
        push @{ $self->{on_created} }, $subref;
    }
}

sub created {
    my $self = shift;

    $self->{is_created} = 1;
    foreach my $callback (@{ $self->{on_created} }) {
        $callback->();
    }
}

=head2 subscribe ($subref, \%opts)

=over 4

Sends a L<Net::AMQP::Protocol::Basic::Consume> frame to the server, storing the $subref as a callback function for when content is received.

Optionally provide %opts which will override defaults for the Basic.Consume call.

The argument signature of the callback is like so:

  my $do_ack = $subref->($message, $meta)

=over 4

=item I<$do_ack>

If in the %opts hash you choose 'no_ack => 0', then messages have to be explicitly ack'ed once handled.  If your callback returns true in this condition, an ack message will automatically be sent for you.

=item I<$message>

Opaque payload of the content body.

=item I<$meta>

Hashref with keys as follows:

=over 4

=item I<method_frame>

L<Net::AMQP::Protocol::Base> delivering method object.

=item I<header_frame>

L<Net::AMQP::Protocol::Base> delivering ContentHeader object.

=item I<weight>, I<body_size>

Copied from the header_frame object.

=item I<payload>

Same as the $message argument above.

=item I<body_frames>

Array of all the L<Net::AMQP::Frame::Body> frames that comprise the payload.

=item I<queue>

The name of this queue object.

=item I<opts>

The options used to create the Basic.Consume call (merge of default values and %opts, above)

=back 

=back

=back

=cut

sub subscribe {
    my ($self, $callback, $user_opts) = @_;
    $user_opts ||= {};

    $self->do_when_created(sub {
        my %opts = (
            ticket       => 0,
            queue        => $self->{name},
            #consumer_tag => '', # auto-generated
            #no_local     => 0,
            no_ack       => 1,
            #exclusive    => 0,
            #nowait       => 0, # do not send the ConsumeOk response
            %$user_opts,
        );

        # TODO: if user sets $opts{nowait}, we can't do the synchronous_callback or even know the consumer_tag.

        $poe_kernel->post($self->{channel}{Alias}, server_send => 
            Net::AMQP::Frame::Method->new(
                synchronous_callback => sub {
                    my $response_frame = shift;
                    my $consumer_tag = $response_frame->method_frame->consumer_tag;
                    if (! $consumer_tag) {
                        print STDERR "Didn't receive a consumer tag to Basic.Consume request\n";
                        return;
                    }
                    $self->{channel}{consumers}{$consumer_tag} = {
                        queue => $self->{name},
                        callback => $callback,
                        opts => \%opts,
                    };
                },
                method_frame => Net::AMQP::Protocol::Basic::Consume->new(%opts)
            ),
        );
    });

    return $self;
}

=head2 publish ($message, \%opts)

=over 4

Sends a message to the queue.  In other words, sends a L<Net::AMQP::Protocol::Basic::Publish> followed by a L<Net::AMQP::Protocol::Basic::ContentHeader> and L<Net::AMQP::Frame::Body> containing the body of the message.

Optionally pass %opts, which can override any option in the L<Net::AMQP::Protocol::Basic::Publish> ('ticket', 'exchange', 'routing_key', 'mandatory', 'immediate'), L<Net::AMQP::Frame::Header> ('weight') or L<Net::AMQP::Protocol::Basic::ContentHeader> ('content_type', 'content_encoding', 'headers', 'delivery_mode', 'priority', 'correlation_id', 'reply_to', 'expiration', 'message_id', 'timestamp', 'type', 'user_id', 'app_id', 'cluster_id') objects.  See the related documentation for an explaination of each.

=back

=cut

sub publish {
    my ($self, $message, $user_opts) = @_;
    $user_opts ||= {};

    $self->do_when_created(sub {
        my %opts = (
            routing_key  => $self->{name}, # route to self
            content_type => 'application/octet-stream',
            %$user_opts,
        );

        $poe_kernel->post($self->{channel}{Alias}, server_send => 
            $self->{channel}{server}->compose_basic_publish($message, %opts)
        );
    });

    return $self;
}

=head2 bind (%opts)

=over 4

Shortcut to send a Queue.Bind call with this queue name.  Pass the same args you'd pass to a L<Net::AMQP::Protocol::Queue::Bind> object creation.

=back

=cut

sub bind {
    my ($self, %opts) = @_;

    $self->do_when_created(sub {
        $opts{queue} ||= $self->{name};
        $poe_kernel->post($self->{channel}{Alias}, server_send =>
            Net::AMQP::Protocol::Queue::Bind->new(%opts)
        );
    });

    return $self;
}

=head1 SEE ALSO

L<POE::Component::Client::AMQP::Channel>

=head1 COPYRIGHT

Copyright (c) 2009 Eric Waters and XMission LLC (http://www.xmission.com/).  All rights reserved.  This program is free software; you can redistribute it and/or modify it under the same terms as Perl itself.

The full text of the license can be found in the LICENSE file included with this module.

=head1 AUTHOR

Eric Waters <ewaters@gmail.com>

=cut

1;
