use Module::Build;

my $build = Module::Build->new(
    module_name => 'POE::Component::Client::AMQP',
    license => 'perl',
    build_requires => {
        'Test::More' => 0,
        'Test::Deep' => 0,
    },
    requires => {
        'Params::Validate' => 0,
        'Class::Accessor' => 0,
        'POE' => 0,
        'POE::Component::Client::TCP' => 0,
        'Net::AMQP' => 0,
    },
    sign => 1,
    create_makefile_pl => 'passthrough',
    create_readme => 1,
);

$build->create_build_script;
