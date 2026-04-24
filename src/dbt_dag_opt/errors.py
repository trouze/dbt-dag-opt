class DbtDagOptError(Exception):
    """Base class for all dbt-dag-opt errors."""


class ArtifactLoadError(DbtDagOptError):
    """Raised when a dbt artifact (manifest / run_results) cannot be loaded."""


class DbtCloudAPIError(ArtifactLoadError):
    """Raised when the dbt Cloud Admin API returns an error response."""

    def __init__(self, message: str, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class InvalidArtifactError(ArtifactLoadError):
    """Raised when an artifact is malformed or missing required keys."""


class GraphError(DbtDagOptError):
    """Raised when the dbt DAG cannot be constructed or is invalid (e.g. cyclic)."""
