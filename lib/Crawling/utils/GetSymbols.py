from typing import Optional, Dict

from lib.Distributor.secretary.models.company import Company
from lib.Distributor.secretary.session import get_session


def get_company_map_from_db(limit: Optional[int] = None) -> Dict[str, dict]:
    with get_session() as session:
        q = session.query(Company.ticker, Company.company_id, Company.cik).order_by(
            Company.company_id.asc()
        )
        if limit:
            q = q.limit(limit)

        return {
            ticker.upper(): {"company_id": company_id, "cik": cik}
            for ticker, company_id, cik in q.all()
            if cik
        }
