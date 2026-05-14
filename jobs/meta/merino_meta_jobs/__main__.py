from merino_meta_jobs.verify import sdk_imports


def main() -> None:
    sdk_imports()
    print("merino_meta_jobs: Meta SDK imports OK")


if __name__ == "__main__":
    main()
