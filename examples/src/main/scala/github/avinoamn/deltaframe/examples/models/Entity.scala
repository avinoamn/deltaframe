package github.avinoamn.deltaframe.examples.models

import com.github.dwickern.macros.NameOf.nameOf

case class Entity(id: String, name: String, age: Int)

object Entity {
  object Columns {
    val idColName: String = nameOf[Entity](_.id)
    val nameColName: String = nameOf[Entity](_.name)
    val ageColName: String = nameOf[Entity](_.age)
  }

  object Data {
    val constantEntities: Seq[Entity] = Seq(
      Entity("A", "Mark", 21),
      Entity("B", "Jacob", 25),
      Entity("C", "Bill", 19),
      Entity("D", "Michael", 27),
    )

    val entitiesForDeletion: Seq[Entity] = Seq(
      Entity("E", "Charlie", 26),
      Entity("F", "Jack", 28),
    )

    val newEntities: Seq[Entity] = Seq(
      Entity("G", "Paul", 22),
      Entity("H", "Jake", 24),
      Entity("I", "Ryan", 31),
    )
  }
}
