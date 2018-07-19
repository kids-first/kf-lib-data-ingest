import pytest

from common.stage import IngestStage


def test_ingest_stage_abs_cls():

    # Declare a concrete ingest stage class
    # class MyGenericIngestStage(IngestStage):

    # Test that the class has implemented all abstractmethods
    # Test that the appropriate exception gets raised if all abstract methods
    # have not been implemented

    pass


def test_validate_parameters():
    # Define dummy extract, transform, load IngestStage classes
    # Test that all of them raise InvalidIngestStageParameters exception
    # when missing or invalid parameters are passed to the run() method
    pass


def test_serialize_output():
    pass


def test_deserialize_output():
    pass
