"""Import checks for the Meta SDK (used by default container CMD)."""


def sdk_imports() -> None:
    from facebook_business.api import FacebookAdsApi  # noqa: F401
    from facebook_business.adobjects.adaccount import AdAccount  # noqa: F401
