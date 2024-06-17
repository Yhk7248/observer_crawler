from dataclasses import dataclass


@dataclass
class KrxPriceData:
    ACC_TRDVAL: str
    ACC_TRDVOL: str
    CMPPREVDD_PRC: str
    FLUC_RT: str
    FLUC_TP_CD: str
    ISU_ABBRV: str
    ISU_CD: str
    ISU_SRT_CD: str
    LIST_SHRS: str
    MKTCAP: str
    MKT_ID: str
    MKT_NM: str
    SECT_TP_NM: str
    TDD_CLSPRC: str
    TDD_HGPRC: str
    TDD_LWPRC: str
    TDD_OPNPRC: str
