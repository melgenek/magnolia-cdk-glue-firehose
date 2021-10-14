package model

import java.util.UUID

final case class PurchaseEvent(id: UUID, products: List[Product], page: Option[Page])
