package otus.tools

case class ClientData(name: String,
                      gender: String,
                      age: Int)

object ClientData {
  def parse(s: String): ClientData = {
    val Array(name,gender,age,_*) = s.split(" ")
    ClientData(name, gender, age.toInt)
  }
}
