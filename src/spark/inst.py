from enum import Enum
from pathlib import Path

import pandas as pd
from tqdm import tqdm


class Inst(Enum):
    """
    A SPARK instrument.
    """

    @property
    def code(self) -> str:
        """
        The instrument code, a unique identifier of the instrument.

        :return: The instrument code.
        """
        return (self.value[0] if isinstance(self.value, tuple) else self.value).upper()

    def get_filepath(self, spark_pathname: str) -> Path:
        """
        Generates the filepath for an instrument using the provided SPARK data release directory.

        :param spark_pathname: The SPARK data release directory, which should end with a date delimited by an underscore.
        :return: The filepath for the instrument.
        """
        date_string = spark_pathname.split("_")[-1]
        spark_path = Path(spark_pathname)
        inst_base_filename = (
            self.value[1] if isinstance(self.value, tuple) else self.value
        )
        inst_filename = f"{inst_base_filename}-{date_string}.csv"
        filepath = spark_path / inst_filename
        return filepath

    def read_csv(self, spark_pathname: str):
        """
        Reads the instrument from disk given the SPARK data release directory.

        :param spark_pathname: The SPARK data release pathname.
        :return: A dataframe containing the instrument data.
        """
        filepath = self.get_filepath(spark_pathname)
        return pd.read_csv(filepath, engine="pyarrow")

    @staticmethod
    def get(
        spark_pathname: str, instruments: list["Inst"] = None
    ) -> dict[str, pd.DataFrame]:
        """
        Reads the specified instruments from disk given the SPARK data release directory.

        :param spark_pathname: The SPARK data release pathname.
        :param instruments: A list of instruments to read. If None, all instruments will be read.
        :return: A dictionary mapping instrument codes to their respective dataframes.
        """
        inst_iter = tqdm(instruments or Inst, desc="Reading instruments")

        return {
            inst.code: inst.read_csv(spark_pathname=spark_pathname)
            for inst in inst_iter
        }

    BMS = ("bms", "basic_medical_screening")
    ACI = ("aci", "approximated_cognitive_impairment")
    ADI = ("adi", "area_deprivation_index")
    ASR = "asr"
    BHA = ("bha", "background_history_adult")
    BHC = ("bhc", "background_history_child")
    BHS = ("bhs", "background_history_sibling")
    CBCL_1_5 = "cbcl_1_5"
    CBCL_6_18 = "cbcl_6_18"
    CLR = ("clr", "clinical_lab_results")
    CDV = ("cdv", "core_descriptive_variables")
    DCDQ = "dcdq"
    IR = ("ir", "individuals_registration")
    IQ = "iq"
    RBSR = "rbsr"
    ROLES = "roles"
    SCQ = "scq"
    SRGD = ("SRGD", "self_reported_genetic_diagnosis")
    SRS_2_ASR = ("srs_2_asr", "srs-2_adult_self_report")
    SRS_2_DA = ("srs_2_DA", "srs2_dependent_adult")
    SRS_2_SA = ("srs_2_sa", "srs2_school_age")
    V3 = ("v3", "vineland-3")
    ADOS_O_1 = ("ados_o_1", "ados/ados_original_module_1")
    ADOS_O_2 = ("ados_o_2", "ados/ados_original_module_2")
    ADOS_O_3 = ("ados_o_3", "ados/ados_original_module_3")
    ADOS_O_4 = ("ados_o_4", "ados/ados_original_module_4")
    ADOS_2_T = ("ados_2_t", "ados/ados_2_toddler")
    ADOS_2_1 = ("ados_2_1", "ados/ados_2_module_1")
    ADOS_2_2 = ("ados_2_2", "ados/ados_2_module_2")
    ADOS_2_3 = ("ados_2_3", "ados/ados_2_module_3")
    ADOS_2_4 = ("ados_2_4", "ados/ados_2_module_4")
