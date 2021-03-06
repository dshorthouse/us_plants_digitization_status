# Apache Spark Set-Up and Queries for iDigBio

Downloaded dataset from iDigBio from the iDigBio download API, generated 2020-07-05
https://api.idigbio.org/v2/download/?rq=

Download status Oct 5
https://api.idigbio.org/v2/download/3586f59b-dd81-48b2-a1ab-f1d70b0325ca

Download status Dec 5
https://api.idigbio.org/v2/download/fbc1e5d5-aa90-479c-b663-af0a6a91e2be

```json
rq = { "basisofrecord": [ "Specimen", "PreservedSpecimen", "FossilSpecimen" ]}
```

```bash
$ brew install apache-spark
$ spark-shell --packages org.apache.spark:spark-avro_2.12:3.0.0 --driver-memory 12G
```

```scala
import sys.process._
import org.apache.spark.sql.Column
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.avro._

// list of recordsets first generated from idigbio_recordsets/idigbio_all_recordsets.rb
// then manually filtered for US-based publishers in idigbio_recordsets/idigbio_us_recordsets.csv & pasted here
var recordsets = List(
  "c4bb08d4-c310-4879-abee-1b3986e8e0ca",
  "77e7e6bd-7822-4b84-a46b-39a06abdac2e",
  "2e3a8e5c-eef9-462d-a690-3a91fe111e13",
  "8295ea14-c4fd-499b-bc68-2907ed36badc",
  "fccc3c1d-d9df-4ffd-b7e1-1b9eb11f95b1",
  "196bb137-2f82-40d5-b294-e730af29749f",
  "b9ab58cf-785e-44a7-a873-1966e14a6715",
  "1f8c7572-5cda-44be-9a78-0658217fc279",
  "67893f3c-c409-41c6-a8f2-47956739a911",
  "d1b25dcd-472e-4902-b53c-3b164269e049",
  "b6d0f953-29b4-41da-a255-2ed07c83edf1",
  "137ed4cd-5172-45a5-acdb-8e1de9a64e32",
  "331b6d1b-842e-4c63-aa23-75ef275d8a9f",
  "94dd2cee-ed7d-4f98-894f-efafeac92b5b",
  "045aa661-f985-4203-80ff-98daafdfe377",
  "aced32f9-e511-48c7-8e9e-b625777bdf7f",
  "7c2c5cdc-80e6-49d5-8e95-08fc7da0a370",
  "82bcf2f2-b2d6-45a5-b0ca-d70d8ab4ebf8",
  "f4f833ea-0abf-4059-948e-132c64dda1be",
  "e2a12ef3-cf03-4ce4-8aff-802ccf2aec1d",
  "781b461a-2788-4fa6-b3df-bbed9447f5a3",
  "2ec3b31e-c86b-4ce9-b265-77c8c3f9643c",
  "b5f4526b-f4fb-4d90-8ce0-975e0cda8ff6",
  "66e00116-15fa-4149-a94a-eb91b98b622c",
  "282fe9c2-d6cb-4325-b3b5-b70ab1d22bbb",
  "2eb8ff2f-4826-4fc3-be68-22d805bcae88",
  "39289378-eed8-442c-ba0b-fce8b1679d8f",
  "6b565194-9707-42da-8052-9f9cf5f9aa60",
  "8919571f-205a-4aed-b9f2-96ccd0108e4c",
  "4960f30c-c1c7-490e-966a-61ad02969e38",
  "cd177f63-761b-44f6-866e-ee19d2ac134e",
  "205fa34c-2fcb-4492-b992-972b18560f6f",
  "9f3bbc7b-c682-4f66-88b3-48eef3a38f38",
  "6c6f34ed-58a4-4ba2-b9c7-34524f79a349",
  "8b0c1f4b-9de7-4521-98c2-983e0bfd33c7",
  "dde625e8-cc2a-4877-9ec4-0b8a20dfded9",
  "ba77d411-4179-4dbd-b6c1-39b8a71ae795",
  "2c662e9e-cdc6-4bbf-93a5-1566ceca1af3",
  "5386d272-06c6-4027-b5d5-d588c2afe5e5",
  "9d9016ef-a88f-4312-a0bf-638fc4be53ba",
  "e881a3e2-f7ba-43c8-ae9a-11fcbfd741bb",
  "a69decc7-76ba-496a-a66e-f91049c02cf0",
  "ec4acc26-ff1b-4131-a1df-a950fe31bb0e",
  "005ac06a-3d9a-46ad-ac3c-062aaa5b7059",
  "0fcbf959-b714-4ba2-8152-0c1440e31323",
  "ef59f7cc-ed42-45fc-9abc-5edfb2c8caec",
  "99e84c7d-dd3c-40d9-9526-54f4342cda95",
  "43c69d2a-0fd2-4d34-a1ef-50d0f9c01353",
  "7340c0df-8829-4197-9dc7-0328b8e7f5dd",
  "ced8c9bc-e8b5-49e7-860a-289fc913860c",
  "8e5fffb5-0b22-472d-8386-de291d17d513",
  "79be41bc-8142-485a-9d57-d6195f9a7c81",
  "b000920c-6f7d-49d3-9d0f-2bb630d2e01a",
  "4520451d-bd3b-4095-b777-24a420d442c1",
  "2c2cc29c-3572-4568-a129-c8cbec34ccbe",
  "49db9725-7bdc-4c38-8548-c32994e811c1",
  "46c11153-2154-495d-89d2-7cdef6425cdb",
  "b8972f6b-c67f-45c0-b348-954866e04a0f",
  "b027383d-6705-4d06-8514-db6ef16efdb5",
  "7f497d81-4c7e-4e06-b166-a459968b14e3",
  "2cebadf7-6d52-49b2-b3a7-d4969a36aa12",
  "e70af26a-fb9e-43ab-96a0-d62a2df37e6d",
  "c481fbc6-4bd7-4c50-8537-ba1993d4eb88",
  "bfb53140-79c1-4625-81aa-3f37de7c0c2f",
  "21b563bc-70c2-46d5-bce8-2489db2db3d8",
  "1527b668-b797-42be-94d3-0058e1393e94",
  "945d6d7d-c768-4189-be9d-f693104d590c",
  "312cf3f1-2913-4c63-9ba8-0b870cd3c120",
  "14f8f83f-7a0c-458c-b6d5-6da7dc8eaa0a",
  "3056112e-97c6-4d0d-b6c2-3c0a9adaca24",
  "844875a9-9927-48a5-90b4-76c5f227f145",
  "e27f0218-47e0-41bc-9086-9d9169096e90",
  "53feaa83-e3b6-4ad3-8597-293b153e7548",
  "781fd581-7b93-471e-a025-413e4bcd8491",
  "a4e6033a-d1eb-46d3-869d-7c0328f09aa7",
  "c13a5966-99a3-4383-b9ef-259cf46800fe",
  "9756b9a4-c070-4359-8a07-2383b09d0d04",
  "8bcb95b2-ab5c-4368-8ead-14588eeb9c98",
  "821c1855-6817-40ee-8732-7f472d238513",
  "ef04e127-bb7d-4bf0-82d3-767d43108f81",
  "41800344-33b3-4201-b9d2-cabbbf564fbc",
  "4dfb5828-3653-4604-ac00-db1e1da98b02",
  "181352ea-3598-4f32-b919-c8f6097f4c65",
  "45544aa4-8762-4bf0-bfc6-890d08dc6ead",
  "241d64f1-480a-48ae-8ec2-cd12af4a16e9",
  "0a854fba-3da1-4d7b-88e1-1204a993ee00",
  "5ab348ab-439a-4697-925c-d6abe0c09b92",
  "9e66257f-21a9-491a-ac23-06b7b62ceeb7",
  "b26fa674-6300-4ea0-a8e3-fc0ce32b5226",
  "8728ef71-fdcc-4027-8139-38b2c0628fba",
  "b985f284-eac5-4efe-8a7c-c726cdf7cf33",
  "9e8c4024-45b1-4c06-854d-9f6d807dae67",
  "9a861ebe-f8d7-4eb1-a2c8-3006f07cfec2",
  "995cc7f1-69c3-4317-ab77-28fd48f1e535",
  "61a1c0ce-8327-4e2a-9766-449751a49b7a",
  "c359c2e5-cd20-4057-9179-35a7a5b5da72",
  "364b1f8d-5975-48d9-bba1-c97ab172986c",
  "fc628e53-5fdf-4436-9782-bf637d812b48",
  "215eeaf0-0a88-409e-a75d-aec98b7c41eb",
  "6aca6f67-a2e9-440d-a503-9501db6e6f36",
  "879d475f-4b76-4d18-8cf6-a7e5a6d44926",
  "de5203b1-5a44-4010-948c-b7d33f46397a",
  "6b5e29d3-b462-44d8-ba38-d68af5088067",
  "6bb853ab-e8ea-43b1-bd83-47318fc4c345",
  "7cc1fb18-45c1-499f-8476-682daa14a4a3",
  "a4b888a2-94bf-4680-b912-84964a236c82",
  "06c35934-1b75-4196-838d-29d509951bf9",
  "6f470382-cb0b-4634-a796-2248bfa97fdc",
  "1e6c8187-1521-4501-b205-ac8f513d5e04",
  "f3327705-8d69-4d9c-88b7-584a08c74653",
  "eeb4872b-7fb4-4ecd-8ce5-82e194f04735",
  "30ab9c2a-0b54-4c04-84ca-bc7abdd90b52",
  "05c029de-734c-450a-a41a-56061b7ebb18",
  "ec248223-f277-4c02-b1fa-60056b5a689a",
  "4d89070b-5dea-4a12-8a09-3f65ba33dba1",
  "b7a79601-c07b-46d5-bd09-d4472b0d9431",
  "36d35b23-113e-4633-90ec-19d265a3b5f6",
  "4c46db9d-74d7-4e45-9ed2-0da40ec6b44f",
  "65536dcd-7bb2-44e5-af3f-4a13f08e53d0",
  "8f3b62fb-56ec-49e8-9f8f-bb257348291f",
  "6eb1dbc4-ea33-456b-8782-0a953900d37a",
  "0220907a-0463-4ae0-8a0b-77f5e80fff40",
  "c5eeb223-0515-423a-a51a-151426c8f60d",
  "4e3043a6-d48a-4a35-b5fb-f67d50cbc158",
  "4c9d08ce-71c1-47b8-a572-2d40e5984c49",
  "9368e302-f8e7-4714-aed4-db2faa861e5c",
  "c6e89321-fc23-4cba-ad79-be3e52edfb6d",
  "c3134980-bf5c-49b8-a289-790d45f02c86",
  "6d4b658a-90b4-4639-8b06-b7f07637f6aa",
  "beecd160-a96c-46fc-bdce-7dcb7024d473",
  "79aa7602-a963-44f3-82dd-e141a387adb8",
  "f2f68b10-b620-4ef8-ac32-c799b38b6d56",
  "e85a9948-9c9e-451d-9485-b2b4cb7b73d5",
  "77b762ba-7cda-4617-97d7-e78df7f6dfab",
  "5889291d-9105-4740-a30f-2d9d2469c264",
  "b1f0612a-bc21-424f-b9c1-3bba69ad4f54",
  "5486b66d-2082-433d-9223-bd789ebca29c",
  "204fbebc-37cc-4331-a2be-11f38949561c",
  "3c367a2d-eec0-4ef1-b3bc-4cbebb320c5a",
  "271a9ce9-c6d3-4b63-a722-cb0adc48863f",
  "f3a5ec1a-49dd-4a52-8bd8-67cacae7a7ac",
  "17cea35c-721f-4d9b-b67f-d29250064d25",
  "83ad8494-136b-485a-87d4-8ce01dd6a8de",
  "e5f57bb0-07ec-4405-90b6-dc89647a1cb5",
  "1720ead7-9d20-4179-8998-2a59b8bfa8d7",
  "e1b03497-7632-4ba4-a9e0-dd230d06638c",
  "0e0e9bbc-1dea-4de4-95ae-aecc90844bbf",
  "1ffce054-8e3e-4209-9ff4-c26fa6c24c2f",
  "0a410c4a-cd4f-4bd8-b6ba-a0c2baa37622",
  "41350373-fc6f-4dd9-b908-27805fff9155",
  "cb33cf97-2a7b-4b45-9b73-5aca568332a6",
  "b12b08da-3d05-4406-a051-0139a33ecf35",
  "6258d160-a7aa-4937-bce3-3538eebd374f",
  "7cea906d-ae65-420c-a6f7-a9a3ad64fb93",
  "959c0dc4-fcf3-477e-af63-c00a005dbc0a",
  "7ad07cff-f782-4ddf-b780-3a757cdb77e0",
  "aca26f37-3ec8-4e9e-b927-50b4944a0096",
  "6226705f-4867-464d-9fab-4e81ecee731f",
  "e34cf41b-196c-4199-85d5-4d2ca5954b09",
  "108cca70-4d1c-4882-843b-2d31ba8d6763",
  "b531ea59-025d-4c29-9d23-99ae75bcd55f",
  "a2a17035-1e6c-46df-9178-a610df825336",
  "341611f4-8b65-4655-b244-9be91a1109cd",
  "0272afc1-36ee-4899-8c28-dde9d8a211d9",
  "7e43ea77-d2e5-4bdc-a4f7-a4792866f53f",
  "f1a78c0f-449c-45fa-9472-0b92cc2a58da",
  "f00b6a32-5337-406b-a850-17f5d78470ad",
  "12018be6-3795-43a3-a073-a2b9d60c0af3",
  "a2beb85e-f2b8-4366-8b3b-e5c5cc117aaf",
  "85e930bf-6e90-4700-85d1-4c3330efbafb",
  "677f57a9-9a0d-4e69-8622-96aa1e6392c2",
  "bdaa6842-3055-465e-82f6-da577e987cde",
  "f630e3ea-697f-404a-8683-b86712c26c43",
  "ee9d38ef-c1db-44de-b8f2-62acb7049370",
  "1b6bb28e-e443-4cd3-910a-c6c43849c2cd",
  "37d4d085-d8be-4826-9bc4-c6a36557fa70",
  "db6c3db9-1e6d-4def-af29-33aa0339bfa9",
  "70abba3e-5f2a-4276-87f3-261706e24453",
  "5277e72c-9e53-4c98-85f1-ee413bc473cd",
  "0fb6618f-f8d6-4361-8b2d-0923d4aa3c09",
  "02fceae6-c71c-4db9-8b2f-e235ced6624a",
  "8783e947-93cf-4b60-b387-d10642b0eee0",
  "de67ccab-7d04-43b9-8083-81e45f628505",
  "aac5fd7f-8043-4aa8-811f-e50de70d96f3",
  "1549b662-ec36-436a-8593-76f7642ec9e4",
  "350ed733-b782-4195-a5dd-e61e5c82d837",
  "667c2736-bcd3-4a6a-abf4-db5d2dc815c4",
  "910eadc8-8131-428c-b28a-91d0e2890f1d",
  "e701ecce-f9ab-445f-afcb-24f279efbc9c",
  "384a1909-f66c-4551-9b26-ea985cd9ccd8",
  "3a0d8092-c577-4775-a586-1542574edc53",
  "5835f642-2560-4e3e-9c25-741a12cc3fe8",
  "8445ab25-ff89-44b0-90f8-bf0790f50afc",
  "d5c32031-231f-4213-b0f1-2dc4bbf711a0",
  "aa53dc91-0cf2-4714-a6e0-f00f9139dcd8",
  "c7ae0ade-c23e-4fe6-a3d4-79bd973374c2",
  "26f7cbde-fbcb-4500-80a9-a99daa0ead9d",
  "333ac26a-30bc-4e0c-a6ef-c57a40f6bd99",
  "c96ca51c-908e-41cc-ae10-ac1fb72ca3d9",
  "347579f4-d44a-4c8e-a578-09c2a8132573",
  "bf9066a2-2c5f-4cf2-821a-1a68b4df5b1b",
  "cd7b335e-6f5c-4259-ba45-5e334a719464",
  "c1e2b821-96a2-422f-a1fe-7a53aaa2e9bf",
  "a6743a43-b86a-4265-9521-fad3a24461a6",
  "9e5aede6-bee5-4a3d-a255-513771b20035",
  "0dab1fc7-ca99-456b-9985-76edbac003e0",
  "5a9ae910-9e4b-488b-af8e-88074fabc3a4",
  "0c15e83e-79ee-4ee3-86e3-e5f98a51dc11",
  "a748a0fe-a6ae-4ce7-b88f-4e4ec1dc080c",
  "5082e6c8-8f5b-4bf6-a930-e3e6de7bf6fb",
  "fe04dab1-5a3d-4c28-a450-012658e982d8",
  "6e922c92-b37d-4c46-8982-19d945ff8fd1",
  "69d150df-eba4-4ab3-9156-71cb0db41830",
  "55e724a4-336a-4315-99e7-01bf0c94f222",
  "bd61c458-b865-4b05-9f1f-735c49066e55",
  "bdf65f9c-a730-4083-bd8d-a2def3037637",
  "1701a75c-5a57-48c3-84c2-234a53f4c3e2",
  "5ddcbd44-1802-46b0-bae5-11126409c03d",
  "fd14095c-3658-4e00-8cec-729a89459e92",
  "93e97f6c-0ab6-41a7-9b58-7e230a80ec1e",
  "5af1bae4-35c0-4ab7-9d08-08bbe22ca003",
  "de56670c-2032-4833-9a89-46f7d6a037c7",
  "58402fe3-37c1-4d15-9e07-0ff1c4c9fb11",
  "bc13ff8e-6f31-4f3d-b47a-38e3ce8a2194",
  "f4bec217-9676-4fc0-be90-856b4b89d4d1",
  "c569e530-7322-40b8-9b66-1e0ed96fefcb",
  "063825dc-b8c3-4962-aea4-9994bcc09bc8",
  "ba54ba45-caac-4708-a389-ac94642976f8",
  "beab5209-9628-4d4d-851e-2bc9bb1a0105",
  "e6eba8cd-fa2c-4ba2-bec0-6841e7633695",
  "caaa464d-e290-4761-8550-75edc6d00119",
  "2941b767-e90b-41b9-9627-6e589e0c0c85",
  "b2b294ed-1742-4479-b0c8-a8891fccd7eb",
  "d2c71720-e156-4943-8182-0a7bbe477a37",
  "176d536a-2691-47c4-95c1-c0d47d3abd48",
  "97058091-eb35-401b-b286-18465761f832",
  "5baa1d0c-cfc0-4c89-8e3e-7a49359b0caa",
  "d6c6e57a-4ccb-4707-b87d-c91d01d6aa42",
  "237bd113-32f3-4091-9710-4a1b074fe26d",
  "a4babe52-5740-44e4-9ea2-acef4797f127",
  "22436fe4-5049-4266-9849-335dd3f161aa",
  "6b2c3ca9-69ad-4316-a2d1-33399e9f547e",
  "91a0a18a-3196-4f87-87b6-02c7f8a12996",
  "2bbafa00-3162-4e8e-947c-64a13b8d3fef",
  "2d4658e3-0d1a-43fc-97ff-b4813dd1f86e",
  "8dfc3d88-8f6b-4432-b69c-534717906004",
  "b62cf6c0-e046-4c3f-9765-d0e046072b0f",
  "e7016bd5-cb10-45b9-8959-0f5750f7a5db",
  "139f2c47-4051-4c44-b95a-45fd20b1a8b9",
  "3872f27e-cf4e-40bd-b91b-ba7b723a86e5",
  "656d1c9a-cbb7-4dde-ac24-62323af5b831",
  "29b8da72-4420-4f65-b755-a006a23cf65a",
  "1c8ec291-8067-4b48-848b-410c2c768420",
  "d14d21fe-da24-47e5-81fb-4bfe962ce828",
  "9c5fea92-a28b-4b6e-94b5-9c939f7369a2",
  "92e4e092-6dcb-46bc-85a0-dea8310aba45",
  "feb74628-8724-466f-8c8c-b3d3b72f2417",
  "0e162e0a-bf3e-4710-9357-44258ca12abb",
  "9c1d51f2-067e-452b-a52b-ea7b03e50e25",
  "18c215c3-c02f-47e9-bb66-196c33c8f672",
  "91c5eec8-0cdc-4be2-9a99-a15ae5ec3edc",
  "1eca069b-09e0-406d-9625-cb9c52e1e5cc",
  "4830ffb8-669a-4717-bec8-2f2374f52120",
  "df22987f-d20d-41db-b8eb-8b5f5fca6df0",
  "8f689b8b-5b65-4638-9555-f2a5d237a624",
  "042dbdba-a449-4291-8777-577a5a4045de",
  "364a24d9-d4a8-4e0b-8e50-07b90f844548",
  "e2def7e2-1455-4856-9823-6d3738417d24",
  "540e18dc-09aa-4790-8b47-8d18ae86fabc",
  "3d4401cb-b3dd-48bd-912a-709bb4ecf5f8",
  "c49bc91d-0a50-497b-8b17-d77808745cf9",
  "1da2a87d-4fc7-4233-b127-59cb8d1ca5ee",
  "2fe53355-8ec5-4d00-ac6f-85d49c2af7a4",
  "ac8c109d-b69c-4359-87f4-8714f8c8a65d",
  "a2105c9c-b869-4637-8850-eb51ea6b1066",
  "b3d53973-5bac-432a-90d3-7956baa09c5d",
  "1f5a1b81-e361-4d65-ab1c-6fb7e30c9910",
  "1e86442f-35a5-4e7b-9a38-4599e4d3b510",
  "1e054b9b-0193-4ff3-b623-9264cf982d4d",
  "d78d13a7-3852-4244-ba34-86ef5765fa99",
  "2936c837-ad9f-40d8-bea0-58b2a635c637",
  "c38b867b-05f3-4733-802e-d8d2d3324f84",
  "e0e37702-32af-405b-b652-ee54b5bb94e2",
  "e4ff51ba-5007-4c40-9a86-e8c6f4db77b7",
  "5e8863ea-56ec-40f3-8075-d42b35d12e72",
  "83fede4d-f1d0-4748-84e5-f24083c7ba3c",
  "a6eee223-cf3b-4079-8bb2-b77dad8cae9d",
  "40987883-03cf-494a-a5cf-7c77c7aadb79",
  "fb4cc282-eba5-4156-b7c7-df41e0581b68",
  "d1983d53-434a-436e-a698-3a2745eb61dc",
  "9ef17d55-0498-44cf-9da4-dde3e3acb570",
  "1ad40bde-8a2a-46bb-9252-0cdc53df5683",
  "71b8ffab-444e-43f9-9a9c-5c42b0eaa5eb",
  "4900d7ce-9442-4305-9889-c9cbbb953eaa",
  "ccba425a-25e0-4d94-8fde-3890be03ae1b",
  "e5b0c46a-5eb6-4b94-9d4c-fb1000f534b0",
  "99b04c9f-908e-42bd-92bc-41aa94b72949",
  "7a31ab80-1cc0-4456-9dea-61c2e9031a6f",
  "09a3fcf2-55a1-488f-aa42-f103bdce0536",
  "0fd6e726-6828-4f62-ba8d-6ec316fe0b52",
  "df516dc6-6ef0-426d-94e3-8a2bbb0439a5",
  "cb2973af-0d5a-4fbf-80ab-ec96ede33ef0",
  "6cab4420-11e4-4b55-85ac-6ecfdda70184",
  "8a54c5fa-2900-4859-a2d6-1b7faedafac4",
  "62254613-2696-4834-8c58-5c465f70df56",
  "e23109b4-e143-437c-b329-3dff7cb35488",
  "f5d395b8-c3c9-43df-b4ff-63d65c6a971e",
  "093030d9-a124-42a8-8cf3-8c6904fefbe7",
  "e794b292-4a94-471e-ab84-6aaef1fea1b3",
  "f4214a7a-6793-48e0-ac41-9baff83096d3",
  "2ee56f8c-db1d-43aa-bfb7-3fb2a19601df",
  "cd5bc13d-ee5c-4b68-a550-37edb3e7899d",
  "1db8b527-7713-405b-b7ec-bc824580ccc6",
  "6ed17163-76e1-48f1-9ccb-19cc462c2639",
  "a16dc8d8-ff4a-4d62-a684-2937fb292b8d",
  "98ece30d-bf85-4122-b872-7786031b457f",
  "f1cf8457-237e-487a-9d13-5de7d81b9de4",
  "1d17fdad-d338-4ae0-9232-dbf18eaf9f66",
  "414a5bf5-061e-4e47-8410-0f76a04f7d1d",
  "837d99c6-3045-4ba3-8951-643ddb3d6676",
  "07c47dd3-a0a9-434a-b144-71c32996f278",
  "1fc07b28-43f5-49d1-badf-63005e1c3e9a",
  "3027c437-cdb3-4072-9410-5a46ec3b1fd5",
  "fb7db48e-1599-420e-8f10-1f20fe1c6aa3",
  "314f66a9-2b8f-4085-b10c-0f083ce2f1eb",
  "90739622-5232-4048-8121-9af9ec69604f",
  "994b60a2-88f1-49ec-a6da-27d56dfa6f16",
  "d07e7b8a-2222-477f-a7f3-f098bbfdaf54",
  "dfd53a42-8f63-4040-93a5-3f1347ce7686",
  "0072bf11-a354-4998-8730-c0cb4cfc9517",
  "7ae4d15d-62e2-459b-842a-446f921b9d3f",
  "3998ec8d-4aae-46db-9370-179c19b69356",
  "a5fdee09-34c4-48bc-99ff-a503c93a9d7e",
  "d3dbe69e-8e8a-4849-9fb1-1bf5c3786f70",
  "7a8d946d-083f-4d2a-9cc9-cd590398194f",
  "7e44229a-e8fa-4570-ada1-0cd7843a66d7",
  "ccd17772-d220-4088-8fa3-df3729f14df4",
  "46374ee7-7c70-48d8-bf16-2e6c1626565e",
  "b3f10d7b-6763-4f79-9789-f6abc68b9720",
  "4bec11d1-f8c3-43a7-9e70-ee0256fcedaf",
  "2f740a87-8049-435d-9d06-1c393c9c11b7",
  "15e04168-22cc-4283-9042-247ab053c7ca",
  "6ac89a16-4604-4a78-9047-dcc57e5c0130",
  "5e893602-84ca-4c8c-bac1-99111c777582",
  "3617a6a3-d384-48de-948d-2d1e1c54e090",
  "5ab5f23d-292e-4bea-ba06-12db0f8a8c86",
  "5e29dbcc-ce45-4f05-9bb0-212baffa8932",
  "a8cafd75-ca3b-42c6-8b8e-52aafa15753b",
  "a4378725-7967-47bc-aada-0220e02e1f96",
  "1ebb0c8e-31f2-4564-b75d-65196bee4f09",
  "3d2b5be0-7c1d-4693-9226-94bc0061123b",
  "401fec56-515d-4fa8-87d1-507e742f4f6f",
  "7e4aacd3-0a24-49ab-b019-518b7069b682",
  "abe3903e-ceba-4864-aa5d-bd985c70fa21",
  "fdf7bb59-aad2-4f10-879f-6c0e7d3baa64",
  "8fc08919-1137-42e4-9fa5-9e64f1e5757b",
  "62c310ac-e1ff-47bc-860d-0471a84ed0d3",
  "664bd710-8791-4dba-a3b6-000a1b140951",
  "86b2bfc9-ca99-4250-b93a-f86f3777236d",
  "1ba0bbad-28a7-4c50-8992-a028f79d1dc5",
  "5a660a44-afdd-45ac-8c48-1a6c570ce0b5",
  "35c43eda-1f4a-4713-bb69-e3fbe1bf792f",
  "2e4ccf50-bb7d-43a6-9640-088b248c2c5a",
  "f9a33279-d6ba-41c7-a511-ef6adfcb6e20",
  "72059315-e131-42ba-b7c6-489415e297b9",
  "570dcca6-a84f-43aa-8053-1a2ac60d9ead",
  "4efa66cf-8adb-414b-b78c-8e651d20f84d",
  "0b17c21a-f7e2-4967-bdf8-60cf9b06c721",
  "ea9de87b-7231-4a05-809f-4b658ea4173d",
  "0bada388-4adf-4b8c-b733-0a1bfc7c233c",
  "cf641fbf-fa31-481a-993b-9204f2ee1884",
  "c9dad611-5e60-4456-934e-75b0e0842ddd",
  "f5c38252-89f1-4753-af3a-da8818fe3a86",
  "0038ce2e-43bb-4f70-8dd6-dca34efd3fca",
  "40250f4d-7aa6-4fcc-ac38-2868fa4846bd",
  "87c45c90-ba1d-409e-a9d7-9baf5a5cbb1c",
  "d53132e6-7997-4850-8607-4fec5a3f9c3f",
  "5626f61a-822e-4692-b432-51f53d053e4d",
  "4b92de1f-866d-4b82-af69-37d46753f289",
  "f84d528a-7d08-467e-b532-ace707316f1d",
  "bf1fee2d-f760-4068-b8e6-d1db63ce434c",
  "0dabd609-2505-4492-8b7a-3f8301d8d5e1",
  "41b119de-f745-482d-be42-a0155bc76e5d",
  "3f508496-c860-4701-93e4-84e940c8395e",
  "b880344e-22b2-4540-a56a-1de5f5601a20",
  "a50e98bd-13e9-41fe-a5cc-aee4f240628b",
  "a83151ae-e1db-4166-9dde-438f6544dca9",
  "471835cc-feb6-4d05-a8d1-62ce71399326",
  "f42a9828-c1d2-47a1-afac-7138e62b3fb1",
  "7110b8ba-0ead-4666-8279-e30f53e343d0",
  "96662fa9-ff60-495b-912a-284f3b98ed72",
  "2c00d087-1df6-4744-807d-056be36eed0d",
  "4523e216-ee13-4b15-a3f7-a6fd56431604",
  "9d2a4189-6048-46e9-bac4-e5ef566334bb",
  "f93c403b-d0f0-4be1-8a4b-ed9aa3b513e3",
  "62f951c1-b6a8-430c-8652-5691f079152c",
  "bbf5f8ed-f33f-40ba-9d0d-1c24dfec4193",
  "120d557c-c5be-474d-98f0-1ba00ae16b40",
  "a0ec3ce6-fc8a-48b7-9105-cecf27602e37",
  "4db72a36-c08b-4a6b-8c68-ab45ebb0efce",
  "e39f6dee-f2cf-4eff-afc9-4600cafe660c",
  "ab820732-0844-4323-ba14-c58c24f3fc7e",
  "7644703a-ce24-4f7b-b800-66ddf8812f86",
  "9b725e43-93c9-423b-adf8-a11d08a83d13",
  "6b546055-ecf5-40dd-a091-67fe6f9531f4",
  "264c48ec-8636-451f-a7e0-74131bc6f84c",
  "7450a9e3-ef95-4f9e-8260-09b498d2c5e6",
  "b7a5dadd-7429-489b-9b3b-72abcd9518a3",
  "ea9d9c05-11b3-4514-898e-66bd8560ec5b",
  "7026b70a-7245-42bd-8162-81ae9a6cfbcb",
  "5975bbda-cd92-4084-8a09-ce1e28e6164f",
  "d29b9265-07e6-4e73-8f72-fc42d3d83fb1",
  "15aa4812-aad2-4b26-a1d8-d4f8d79e6163",
  "65007e62-740c-4302-ba20-260fe68da291",
  "ddef79ec-043c-4027-9876-c4a298feff6d",
  "72d4c3c7-3413-4588-a803-e1a63e0d7c6c",
  "15a1cc29-b66c-4633-ad9c-c2c094b19902",
  "43aa1339-67e4-4298-b7c5-3d0f201266ef",
  "fb97dfb4-72be-4dc1-9f5a-2faea75341b4",
  "1bb33d2d-0714-4fc9-968e-b66bab1cf3d3",
  "03e04b8e-1dea-49ac-8a46-4276ddcfed21",
  "c3a7b339-122e-4fe9-8851-371b1fdf584e",
  "50ca2a08-0e76-4f56-9976-d344dd201a9b",
  "d11f19ae-e946-4a0e-83e5-2052ae8cca62",
  "662b1aa5-9c19-4eb9-9766-1da78a117456",
  "03eac319-23c7-429e-9fa4-480640007d62",
  "459bd81e-63b9-47d2-9818-dceb6657bea0",
  "dd1b8c0b-280f-4cd3-979a-ad7464c5c9b5",
  "59734d15-8edb-41a4-b3f2-f9bd3407460b",
  "41e1a09b-bd55-4d20-a480-5d8187f7afca",
  "40d35f62-cf89-488a-bad3-66e66d38c10c",
  "c5b7dae8-b483-4742-9954-d7f9226daa34",
  "44328ef7-fc7f-4ff6-b51b-ed9049857e11",
  "d621e959-2633-4ec1-a2a2-5d97cd818b47",
  "8dc14464-57b3-423e-8cb0-950ab8f36b6f",
  "0bc60df1-a162-4173-9a73-c51e09031843",
  "6cc31636-c3e5-4887-bbee-2e56621771c4",
  "9b34b218-efb2-43b9-9b9e-dac3c470a9f9",
  "7c927849-94ed-4034-90e9-af34ac0cb47c",
  "e95396c4-1cac-4c9b-b461-5f21cd978fc6",
  "9a09745d-4449-46a2-b8b3-60f3c0d25e83",
  "f33e9494-7b3c-4ac6-a735-59693b5a9638",
  "b00cf471-6bbe-4f94-846e-288900398b65",
  "dd232f5c-7f53-48ec-9bb7-7205702c3dc8",
  "1f2b44b8-8556-4d6e-8247-4611689551cf",
  "76fd34da-4892-4821-858d-98fe9e28ba8b",
  "e7db896a-c95b-4a18-99a4-866fff238ca5",
  "d81c6ad6-fb8f-4c31-bba3-f2b65f780893",
  "9b62118d-9b90-46b1-854d-06a5a9a22a90",
  "ef637c01-c551-4d47-8a48-4442b8ad5ecd",
  "aeed0286-ffe4-45b8-a7b9-bcee18361433",
  "9835e4d7-a817-430f-9f55-5fc3325e4399",
  "b5e5c781-765f-4981-af2a-c19c250e2cf0",
  "4cdf5c2f-1a44-4fd5-bdd8-de08c8a660e2",
  "db3181c9-48dd-489f-96ab-a5888f5a938c",
  "d6f12b28-bb9f-44f9-9000-08f644658bf8",
  "4f436daa-01d5-4be6-b5c3-fdd255677536",
  "af60ac71-fbfb-4648-95fb-0eb5fe24765b",
  "c882214e-8ba4-482b-9998-070b5547dc54",
  "9a06cc34-be24-4ebf-b599-cbb1d4b8ac7b",
  "767c78b4-1c16-4cac-ad04-66333ac5a7f2",
  "23b85d9d-4669-40db-901f-aaad686fe0b8",
  "25cd5e12-7830-4f46-bf6d-9b6deb706f44",
  "23a47a5f-2ac1-4f81-acd3-21d5b82ed22a",
  "531537fc-6349-4a20-ae42-540d61797086",
  "f89ada44-88df-46f0-bc61-cc46d9c84673",
  "f0599190-e5b7-42ed-bec8-810905f50c34",
  "2d47501f-dbed-43ce-a9c3-9c8542648ce4",
  "33fd0737-6207-42cc-bc64-cc637266b476",
  "e7ac8c4a-64bd-491b-b764-232de9b4bfe5",
  "85ae9fb4-de87-41ce-abb3-44fda2fb24a8",
  "a2b36fdf-50bc-44ef-a6a4-ca6dc1dc148a",
  "5ffc8bc6-e366-4157-b1ba-00859f1048a4",
  "538bdd33-b616-4825-8542-7033cc8a185f",
  "7b0809fb-fd62-4733-8f40-74ceb04cbcac",
  "64b54f96-91f0-4442-b6de-173aa1a5c31b",
  "8e4ff036-d38e-455f-a0fe-4ac50823f4fb",
  "b6ec6203-09db-4d6e-8cba-ee4bebd2934c",
  "09edf7d2-e68e-4a42-93da-762f86bb814f",
  "a6e02b78-6fc6-4cb6-bb87-8d5a443f2c2a",
  "04d9b721-259c-4d6b-b48f-2e23edf66c9f",
  "9c963109-9898-4953-a351-d5ee36d6115b",
  "71e55d91-4c0c-4a13-9421-c732348b0a47",
  "5f681b65-9a7e-4b79-8a17-fc95cc26b837",
  "0f19f6d6-79a4-434e-ba0b-a4f49f334078",
  "d5a1b706-c624-43df-afcf-9cea7094e75b",
  "8e2f9392-55ac-49f3-bcc2-131a00122626",
  "7809d96b-7edf-4ef7-9f12-59967e9a01a6",
  "a2a7754b-2346-496d-b681-eb754ef32b9e",
  "be31dfd1-c721-4697-8ee0-f7043c070810",
  "e36691ec-c4f8-4bec-b331-b48ffa82ff49",
  "e60301d9-9b40-483f-92de-065769b9d3dd",
  "dad057a8-0648-4e11-9652-740ce7f86d62",
  "ff0d1543-247c-4039-a8ff-cf4fc224c449",
  "2b45c778-b10c-496b-b8cb-1e414da59ccf",
  "13510466-6232-4313-9e46-ea197b82750c",
  "997c4dda-0465-40c0-af8d-67f8f90dea3b",
  "20360fae-574a-4d63-b9f6-47b1cc07fd22",
  "3feab0ae-cfc3-40fb-b681-018c410f1996",
  "645bcd10-a74f-4207-a375-0254b954b7ad",
  "3501e0a6-1420-45b9-bbf9-77349e79e9d7",
  "e5f377de-c662-4eec-a4d4-b41b18441f3a",
  "f08c31ea-0e90-4cc1-b471-dfd0584ae7cf",
  "e1c7bc41-50a4-4723-b8b3-f970844ffb65",
  "f0174bc9-0cca-450e-a941-655d80040139",
  "d478c23a-1993-443d-85ea-308870606626",
  "ff111763-e72d-4f24-8914-b5b2dd94908c",
  "be34dbd9-5d54-4837-9f49-ff423eb18e8b",
  "954ec5e0-4fcd-414d-8ad2-46b4b75cfc74",
  "c5d42fed-eed0-4e14-9625-f8a9c0ff6bb1",
  "72b6dbee-bc4d-4e35-a32c-8df0422771fb",
  "fc40fabd-0a70-48fa-b142-79990cd259a5",
  "cf42855e-a54a-4488-a79e-beac086ba1d4",
  "a062eb42-d5c6-4332-8c88-64b4ac1af892",
  "9ff03c57-ba5a-4127-9439-4bf3e838c4df",
  "4ef6540c-e6cf-4678-bc43-b57435354de0",
  "b1c7a275-21f6-4b66-895d-d497359b34a1",
  "d7b285d4-2643-45ee-9302-b0c3d51dda5c",
  "b7349341-c8e2-4628-be5f-77600ba730fa",
  "6d09cfc1-a17c-4067-b1a0-557b8e5334ea",
  "cc11b4e3-823d-4490-95b2-afe6a1f3a9d2",
  "95ecb448-3c1f-4145-8565-4f6d51beb62c",
  "2c00c297-9ebd-498a-b701-d3ebde4b49f3",
  "433d3c37-8dde-42e4-a344-2cb6605c5da2",
  "3c6f1ea5-f2e7-4203-9cfe-74ec2fb1b035"
)

var kingdoms = List(
  "plantae",
  "chromista",
  "fungi"
)

// List of terms we care about
val occurrence_terms = List(
  "coreid",
  "idigbio:uuid",
  "dwc:institutionCode",
  "dwc:collectionCode",
  "dwc:catalogNumber",
  "dwc:occurrenceID",
  "dwc:kingdom",
  "dwc:scientificName",
  "dwc:typeStatus",
  "idigbio:recordset",
  "dwc:continent",
  "idigbio:isoCountryCode",
  "dwc:stateProvince",
  "dwc:county",
  "dwc:locality",
  "dwc:recordedBy",
  "dwc:recordNumber",
  "dwc:eventDate",
  "idigbio:geoPoint",
  "idigbio:hasImage"
)

//Build DataFrame from occurrence.csv; prefix "o_" on columns stands for "occurrence_"
val occurrence = spark.
    read.
    format("csv").
    option("header", "true").
    option("mode", "DROPMALFORMED").
    option("delimiter", ",").
    option("quote", "\"").
    option("escape", "\"").
    option("treatEmptyValuesAsNulls", "true").
    option("ignoreLeadingWhiteSpace", "true").
    load("/Users/dshorthouse/Downloads/GBIF/occurrence.csv").
    select(occurrence_terms.map(col):_*).
    filter(lower($"dwc:kingdom").isin(kingdoms:_*)).
    filter($"idigbio:recordset".isin(recordsets:_*)).
    withColumnRenamed("idigbio:uuid", "o_uuid").
    withColumnRenamed("idigbio:recordset", "o_recordSet").
    withColumnRenamed("dwc:institutionCode", "o_institutionCode").
    withColumnRenamed("dwc:collectionCode", "o_collectionCode").
    withColumnRenamed("dwc:catalogNumber", "o_catalogNumber").
    withColumnRenamed("dwc:occurrenceID", "o_occurrenceID").
    withColumnRenamed("dwc:kingdom", "o_kingdom").
    withColumnRenamed("dwc:scientificName", "o_scientificName").
    withColumnRenamed("dwc:typeStatus", "o_typeStatus").
    withColumnRenamed("dwc:continent", "o_continent").
    withColumnRenamed("dwc:stateProvince", "o_stateProvince").
    withColumnRenamed("dwc:county", "o_county").
    withColumnRenamed("dwc:locality", "o_locality").
    withColumnRenamed("idigbio:isoCountryCode", "o_isoCountryCode").
    withColumnRenamed("dwc:recordNumber", "o_recordNumber").
    withColumnRenamed("dwc:recordedBy", "o_recordedBy").
    withColumnRenamed("dwc:eventDate", "o_eventDate").
    withColumnRenamed("idigbio:geoPoint", "o_hasCoordinate").
    withColumnRenamed("idigbio:hasImage", "o_hasImage").
    withColumnRenamed("coreid", "o_coreid").
    withColumn("o_hasImage", when($"o_hasImage".contains("true"), 1).otherwise(lit(null))).
    withColumn("o_hasCoordinate", when($"o_hasCoordinate".isNotNull, 1).otherwise(lit(null)))

//optionally save the DataFrame to disk so we don't have to do the above again
occurrence.write.mode("overwrite").format("avro").save("idigbio_plants_occurrence_avro")

//load the occurrence DataFrame, can later skip the above processes and start from here
val occurrence = spark.
    read.
    format("avro").
    load("idigbio_plants_occurrence_avro")

//list of terms we want from occurrence_raw.csv
var occurrence_raw_terms = List(
  "coreid",
  "dwc:institutionCode",
  "dwc:collectionCode",
  "dwc:catalogNumber",
  "dwc:occurrenceID",
  "dwc:identifiedBy",
  "dwc:dateIdentified",
  "dwc:acceptedNameUsage",
  "dwc:preparations",
  "dwc:scientificNameID",
  "dwc:identificationID",
  "dwc:higherGeography",
  "dwc:decimalLatitude",
  "dwc:decimalLongitude",
  "dwc:verbatimElevation",
  "dwc:verbatimDepth"
)

//Build dataframe from occurrence_raw.csv; prefix "or_" on columns stands for occurrence_raw
val occurrence_raw = spark.
  read.
  format("csv").
  option("header", "true").
  option("mode", "DROPMALFORMED").
  option("delimiter", ",").
  option("quote", "\"").
  option("escape", "\"").
  option("treatEmptyValuesAsNulls", "true").
  option("ignoreLeadingWhiteSpace", "true").
  load("/Users/dshorthouse/Downloads/GBIF/occurrence_raw.csv").
  select(occurrence_raw_terms.map(col):_*).
  withColumnRenamed("dwc:institutionCode", "or_institutionCode").
  withColumnRenamed("dwc:collectionCode", "or_collectionCode").
  withColumnRenamed("dwc:catalogNumber", "or_catalogNumber").
  withColumnRenamed("dwc:occurrenceID", "or_occurrenceID").
  withColumnRenamed("dwc:identifiedBy", "or_identifiedBy").
  withColumnRenamed("dwc:dateIdentified", "or_dateIdentified").
  withColumnRenamed("dwc:acceptedNameUsage", "or_acceptedNameUsage").
  withColumnRenamed("dwc:preparations", "or_preparations").
  withColumnRenamed("dwc:scientificNameID", "or_scientificNameID").
  withColumnRenamed("dwc:identificationID", "or_identificationID").
  withColumnRenamed("dwc:higherGeography", "or_higherGeography").
  withColumnRenamed("dwc:decimalLatitude", "or_decimalLatitude").
  withColumnRenamed("dwc:decimalLongitude", "or_decimalLongitude").
  withColumnRenamed("dwc:verbatimElevation", "or_verbatimElevation").
  withColumnRenamed("dwc:verbatimDepth", "or_verbatimDepth").
  withColumnRenamed("coreid", "or_coreid")

occurrence_raw.write.mode("overwrite").format("avro").save("idigbio_plants_occurrence_raw_avro")

//load the occurrence_raw DataFrame, can later skip the above processes and start from here
val occurrence_raw = spark.
    read.
    format("avro").
    load("idigbio_plants_occurrence_raw_avro")

val idigbio = occurrence.
    join(occurrence_raw, $"o_coreid" === $"or_coreid", "inner")

idigbio.write.mode("overwrite").format("avro").save("idigbio_plants_combined_avro")

//load the occurrence_raw DataFrame, can later skip the above processes and start from here
val idigbio = spark.
    read.
    format("avro").
    load("idigbio_plants_combined_avro")

idigbio.groupBy("or_institutionCode").
    agg(
      count("o_coreid").alias("total"),
      count("or_collectionCode").alias("has_collectionCode"),
      count("or_catalogNumber").alias("has_catalogNumber"),
      count("or_preparations").alias("has_preparations"),
      count("o_scientificName").alias("has_scientificName"),
      count("or_scientificNameID").alias("has_scientificNameID"),
      count("or_acceptedNameUsage").alias("has_acceptedNameUsage"),
      count("or_identificationID").alias("has_identificationID"),
      count("o_locality").alias("has_locality"),
      count("or_higherGeography").alias("has_higherGeography"),
      count("o_continent").alias("has_continent"),
      count("o_isoCountryCode").alias("has_countryCode"),
      count("o_stateProvince").alias("has_stateProvince"),
      count("o_county").alias("has_county"),
      count("or_decimalLatitude").alias("has_decimalLatitude"),
      count("or_decimalLongitude").alias("has_decimalLongitude"),
      count("o_hasCoordinate").alias("has_coordinates"),
      count("or_verbatimElevation").alias("has_verbatimElevation"),
      count("or_verbatimDepth").alias("has_verbatimDepth"),
      count("o_hasImage").alias("has_image"),
      count("or_dateIdentified").alias("has_dateIdentified"),
      count("or_identifiedBy").alias("has_identifiedBy"),
      count("o_recordedBy").alias("has_recordedBy"),
      count("o_eventDate").alias("has_eventDate"),
      count("o_recordNumber").alias("has_recordNumber")
    ).
    repartition(1).
    write.
    mode("overwrite").
    option("header", "true").
    option("quote", "\"").
    option("escape", "\"").
    csv("idigbio_institutionCode_summmary")

```
