sub MAIN(Str :$host = '0.0.0.0', Int :$port = 3333, :$file, Int :$interval = 60) {

    my $vin = 'LSJA0000000000091';
    my $last_meter = 0;

    react {
        whenever IO::Socket::Async.listen($host, $port) -> $conn {
            if $file.defined {
                my @lines = $file.IO.lines;
                for @lines -> $line {
                    print sprintf("%s\n", $line);
                    $conn.print: sprintf("%s\n", $line);
                }
            }
            react {
                my Bool:D $ignore = True;

                whenever Supply.interval($interval).rotor(1, 1 => 1) {
                    $ignore = !$ignore;
                }

                whenever Supply.interval(1) {
                    next if $ignore;
                    my $now = (DateTime.now.Instant * 1000).Int;
                    print sprintf("\{'vin':'%s','createTime':%s,'mileage':%s}\n", $vin, $now, $last_meter);
                    $conn.print: sprintf("\{'vin':'%s','createTime':%s,'mileage':%s}\n", $vin, $now, $last_meter++);
                }

                whenever signal(SIGINT) {
                    say "Done.";
                    done;
                }
            }
        }
        CATCH {
            default {
                say .^name, ': ', .Str;
                say "handled in $?LINE";
            }
        }
    }
}
