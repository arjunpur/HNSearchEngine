package client

import com.sun.javafx.util.Logging
import org.scalatest.FunSuite
import org.mockito.Mockito._

/**
  * Created by arjunpuri on 11/27/16.
  */
class ClientTest extends FunSuite {

  private val clientStub = spy(new HNClient())

  test("Client parses item correctly") {

    val json =
    """{"by":"mattculbreth",
      |"id":500,
      |"kids":[501,454940],
      |"parent":493,
      |"text":"Actually, why not do it here?  I bet that sort of functionality could be integrated somehow into this site, and from Paul's first post to the group on the reasons for the site I think it would fit in well.\r\n\r\nThen again you won't be able to use Ruby...",
      |"time":1172164181,
      |"type":"comment"}""".stripMargin

    doReturn(json).when(clientStub).execute(ItemRequest(500L).url)

    assert(clientStub.item(500L) ===  Post("mattculbreth", 500, List(501, 454940), 493,
      "Actually, why not do it here?  I bet that sort of functionality could be integrated somehow into this site, and from Paul's first post to the group on the reasons for the site I think it would fit in well.\r\n\r\nThen again you won't be able to use Ruby..."
      , 1172164181, "comment"))

  }

}
