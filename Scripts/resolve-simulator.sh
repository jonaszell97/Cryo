#!/bin/sh

set -eu

simulator_name=${1:?A simulator name is required}
simulator_os=${2:-}
simulator_list=$(xcrun simctl list devices available)
current_runtime=

while IFS= read -r line; do
    case "$line" in
        "-- "*" --")
            current_runtime=${line#-- }
            current_runtime=${current_runtime% --}
            continue
            ;;
    esac

    if [ -n "$simulator_os" ]; then
        case "$current_runtime" in
            *"$simulator_os"*) ;;
            *) continue ;;
        esac
    fi

    trimmed_line=${line#"${line%%[![:space:]]*}"}
    prefix="$simulator_name ("
    case "$trimmed_line" in
        "$prefix"*)
            remainder=${trimmed_line#"$prefix"}
            simulator_id=${remainder%%)*}
            printf '%s\n' "$simulator_id"
            exit 0
            ;;
    esac
done <<EOF
$simulator_list
EOF

if [ -n "$simulator_os" ]; then
    printf 'No available simulator named "%s" was found for OS %s.\n' "$simulator_name" "$simulator_os" >&2
else
    printf 'No available simulator named "%s" was found.\n' "$simulator_name" >&2
fi
exit 1
