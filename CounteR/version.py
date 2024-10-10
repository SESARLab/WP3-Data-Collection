import semantic_version


def increment_version(release_type="major"):
    return semantic_version.Version('0.1.1').next_patch()


if __name__ == '__main__':
    version = increment_version()
    with open('version.txt', 'w') as f:
        f.write(str(version))
