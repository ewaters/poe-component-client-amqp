package POE::Component::Client::AMQP::Queue;

use strict;
use warnings;
use POE;
use Params::Validate;

sub create {
    my $class = shift;

    my %self = validate(@_, {
        name    => 1,
        channel => 1,

        is_created => { default => 0 },
        on_created => { default => [] },
    });

    return bless \%self, $class;
}

sub created {
    my $self = shift;

    $self->{is_created} = 1;
    while (my $callback = shift @{ $self->{on_created} }) {
        $callback->();
    }
}

sub subscribe {
    my ($self, $callback) = @_;

    my $action = sub {
        $poe_kernel->post($self->{channel}{Alias}, server_send => 
            Net::AMQP::Protocol::Basic::Consume->new(
                ticket => 0,
                queue => $self->{name},
                consumer_tag => '', # auto-generated
                no_local => 0,
                no_ack   => 0,
                exclusive => 1,
                nowait    => 0, # do not send the ConsumeOk response
            )
        );
    };

    if ($self->{is_created}) {
        $action->();
    }
    else {
        push @{ $self->{on_created} }, $action;
    }
}

sub publish {
    my ($self, $message) = @_;

    my $action = sub {
        $poe_kernel->post($self->{channel}{Alias}, server_send => 
            Net::AMQP::Protocol::Basic::Publish->new(
                ticket      => 0,
                exchange    => '', # default exchange
                routing_key => $self->{name}, # route to my queue
                mandatory   => 1,
                immediate   => 0,
            ),
            Net::AMQP::Protocol::Basic::ContentHeader->new(
                raw_frame_options => {
                    weight         => 1,
                    body_size      => length($message),
                    property_flags => {},
                },

                content_type     => '',
                content_encoding => '',
                headers          => {},
                delivery_mode    => 1, # non-persistent
                priority         => 0,
                correlation_id   => '',
                reply_to         => '',
                expiration       => '',
                message_id       => '',
                timestamp        => time,
                type             => '',
                user_id          => '',
                app_id           => '',
                cluster_id       => '',
            ),
            Net::AMQP::Protocol::BaseBody->new(payload => $message),
        );
    };

    if ($self->{is_created}) {
        $action->();
    }
    else {
        push @{ $self->{on_created} }, $action;
    }
}

1;
