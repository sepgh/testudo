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


def parse_internal(binary: str, degree: int):
    print()
    print("'%-10s'" % "0-2", "", binary[0:2])
    print()

    offset = 2

    # First child pointer
    print(
        "'%-10s'" % f"{offset}-{offset+26}",
        "",
        binary[offset: offset + 2],
        binary[offset + 2: offset + 18],
        binary[offset + 18: offset + 26],
    )

    offset += 26

    for i in range(0, degree - 1):
        print(
            "'%-10s'" % f"{offset}-{offset+16}",
            "",
            binary[offset: offset + 16],
        )
        print(
            "'%-10s'" % f"{offset+16}-{offset+42}",
            "",
            binary[offset + 16: offset + 18],
            binary[offset + 18: offset + 34],
            binary[offset + 34: offset + 42],
        )
        offset += 42

    print()
    a = binary[offset:]
    print("'%-10s'" % f"{offset}-...", "", ' '.join([a[i:i+4] for i in range(0, len(a), 4)]))


def is_leaf() -> bool:
    _in = input("Leaf? y/n ")
    if _in == "y" or _in == "Y":
        return True
    if _in == "n" or _in == "N":
        return False
    print("Answer with Y or N")
    return is_leaf()


if __name__ == "__main__":
    is_leaf_node = is_leaf()
    degree = int(input("degree: "))
    data = input("data: ")
    if is_leaf_node:
        parse_leaf(data, degree)
    else:
        parse_internal(data, degree)

