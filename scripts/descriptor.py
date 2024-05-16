import sys


def parse_leaf(binary: str, degree: int):
    print()
    print("'%-10s'" % "0-2", "", binary[0:2])
    print()

    offset = 2
    for i in range(0, degree - 1):
        print(
            "'%-10s'" % f"{offset}-{offset+42}",
            "",
            binary[offset: offset + 16],
            binary[offset + 16: offset + 18],
            binary[offset + 18: offset + 34],
            binary[offset + 34: offset + 42]
        )
        offset += 42
    print()
    for _ in range(0, 2):
        print(
            "'%-10s'" % f"{offset}-{offset+26}",
            "",
            binary[offset: offset + 2],
            binary[offset + 2: offset + 18],
            binary[offset + 18: offset + 26],
        )
        offset += 26

    print()
    a = binary[offset:]
    print("'%-10s'" % f"{offset}-...", "", ' '.join([a[i:i+4] for i in range(0, len(a), 4)]))


if __name__ == "__main__":
    parse_leaf(sys.argv[2], int(sys.argv[1]))
