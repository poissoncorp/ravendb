﻿using System.Runtime.InteropServices;
using Raven.Client.Documents.Queries.Vector;
using Raven.Server.Config;
using Tests.Infrastructure;
using Voron.Data.Graphs;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax.Vectors;

public class Int8VectorSimilarityTests : RavenTestBase
{
    public Int8VectorSimilarityTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void Test(Options options)
    {
        options.ModifyDatabaseRecord += record =>
        {
            record.Settings[RavenConfiguration.GetKey(x => x.Indexing.CoraxIncludeDocumentScore)] = true.ToString();
        };

        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                var dto1 = new Dto() { EmbeddingSingles = Vector1 };
                var dto2 = new Dto() { EmbeddingSingles = Vector2 };

                session.Store(dto1);
                session.Store(dto2);

                session.SaveChanges();

                var res = session.Advanced
                    .RawQuery<QueryResult>(
                        "from 'Dtos' v where vector.search(embedding.f32_i8('EmbeddingSingles'), $p0) order by score() select { Score: v['@metadata']['@index-score'] }")
                    .AddParameter("$p0", Vector1).ToList();
                WaitForUserToContinueTheTest(store);
                Assert.Equal(2, res.Count);
                Assert.True(res[0].Score > 0.98);
                Assert.True(res[1].Score > 0.85);
            }
        }
    }

    private class QueryResult
    {
        public float Score { get; set; }
    }

    private class Dto
    {
        public float[] EmbeddingSingles { get; set; }
    }

    private static float[] Vector1 =>
    [
        0.21250519156455994f, 0.4327700138092041f, 0.7406141757965088f, 0.2592349052429199f, -0.019379794597625732f, -0.04277557134628296f, 0.3835868835449219f,
        -0.2594020366668701f, 0.3042496144771576f, 0.3051306903362274f, -0.38539648056030273f, 0.05074748396873474f, -0.2500429153442383f, 0.39396965503692627f,
        -0.5134544372558594f, -0.36124086380004883f, 0.3874073028564453f, 0.4314107894897461f, -0.3940984904766083f, -0.018799349665641785f, -0.2851676940917969f,
        0.08034932613372803f, 0.4435997009277344f, -0.10967850685119629f, 0.4846153259277344f, -0.3041456639766693f, -0.1745615303516388f, 0.1556938886642456f,
        -0.7720985412597656f, 0.2938051223754883f, 0.34822654724121094f, -0.009625378996133804f, -0.01608404517173767f, 0.6687707901000977f, -0.7508449554443359f,
        0.19582697749137878f, 0.23862266540527344f, -0.057160913944244385f, -0.030317246913909912f, -0.04366648197174072f, 0.158452570438385f,
        0.2793159484863281f, -0.13134101033210754f, -0.011792033910751343f, 0.4005317687988281f, -0.01658240705728531f, -0.13833087682724f, 0.08331811428070068f,
        0.06140536069869995f, 0.000054001808166503906f, -0.1127886176109314f, -0.15187615156173706f, -0.3792896270751953f, -0.20846271514892578f,
        0.006319820880889893f, -0.08936912566423416f, -0.30371320247650146f, 0.5090465545654297f, -0.11715102195739746f, 0.1405423879623413f,
        -0.02518153190612793f, 0.406829833984375f, -0.08293033391237259f, -0.47765350341796875f, -0.1548646241426468f, 0.4428367614746094f, 0.09995809942483902f,
        0.22783970832824707f, -0.1963314563035965f, 0.5667381286621094f, 0.11118939518928528f, 0.09095411747694016f, 0.34425926208496094f, 0.3313429355621338f,
        0.11000099778175354f, 0.12535256147384644f, 0.08623012900352478f, -0.30995112657546997f, 0.3382077217102051f, 0.29703474044799805f, 0.037961915135383606f,
        0.012613087892532349f, 0.18099495768547058f, 0.6784076690673828f, 0.08348464965820312f, 0.424530029296875f, 0.11805307865142822f, -0.0456102192401886f,
        0.29816198348999023f, 0.2888205051422119f, -0.4065895080566406f, -0.06593042612075806f, -0.38208216428756714f, 0.0009989142417907715f,
        -0.15270662307739258f, 0.3338313698768616f, 0.5468940734863281f, -0.4507160186767578f, -0.2252197265625f, -0.514251708984375f, -0.0012141764163970947f,
        -0.28244972229003906f, -0.3686332702636719f, 0.19455602765083313f, -0.1265038251876831f, -0.2737302780151367f, -0.06465324759483337f,
        0.40096330642700195f, 0.1796419620513916f, 0.3612098693847656f, -0.29494285583496094f, -0.11924433708190918f, 0.067757248878479f, -0.013638734817504883f,
        0.3406343460083008f, 0.1336943358182907f, -0.22100305557250977f, 0.12484045326709747f, -0.034911274909973145f, -0.9160652160644531f, 0.2491741180419922f,
        0.09576840698719025f, -0.310535192489624f, -0.31558915972709656f, 0.6575088500976562f, -0.1314471960067749f, 0.5413856506347656f, -0.27504968643188477f,
        0.5197467803955078f, 0.34899580478668213f, -0.3631744682788849f, -0.25406360626220703f, 0.7834281921386719f, 0.3582420349121094f, 0.40657132863998413f,
        0.35531747341156006f, -0.0945749282836914f, 0.4344329833984375f, -0.13738799095153809f, -0.7176132202148438f, -0.027093887329101562f,
        -0.08545772731304169f, 0.6711463928222656f, 0.5741419792175293f, -0.2052311897277832f, 0.1285456418991089f, 0.04801423102617264f, 0.6753501892089844f,
        0.10414528846740723f, 0.17312240600585938f, 0.07124733924865723f, -0.14620769023895264f, 0.08079130947589874f, 0.41997337341308594f, -0.3269079029560089f,
        0.3589465618133545f, 0.3135581314563751f, -0.0024099349975585938f, 0.21088701486587524f, -0.2890435457229614f, 0.8106575012207031f, 0.3590583801269531f,
        0.11862817406654358f, -0.11961992084980011f, -0.3715076446533203f, 0.15027576684951782f, -0.0009207790717482567f, 0.4515190124511719f,
        0.37869226932525635f, -0.3665693998336792f, -0.3492269515991211f, 0.0709877610206604f, 0.22082507610321045f, 0.02830454707145691f, 0.19855904579162598f,
        0.1861565113067627f, -0.0813136100769043f, 0.1004500687122345f, -0.09772306680679321f, -0.03328418731689453f, 0.21103334426879883f, 0.21512262523174286f,
        -0.16121652722358704f, 0.40160036087036133f, 0.5058727264404297f, 0.17190730571746826f, 0.46714353561401367f, 0.19864483177661896f, -0.20427274703979492f,
        0.211357980966568f, -0.1343260556459427f, -0.06702589988708496f, 0.4346599578857422f, -0.22504359483718872f, 0.17280694842338562f, 0.28623393177986145f,
        -0.5381642580032349f, 0.8164482116699219f, -0.17371287941932678f, 0.4661121368408203f, 0.26131534576416016f, 0.1401086002588272f, -0.19661879539489746f,
        0.023643165826797485f, 0.4255943298339844f, -0.58266282081604f, 0.24780187010765076f, 0.03917139768600464f, 0.05494129657745361f, -0.03909432888031006f,
        -0.18806159496307373f, 0.1512761116027832f, 0.9322299957275391f, 0.30248427391052246f, -0.2547307014465332f, 0.2583366632461548f, -0.5698986053466797f,
        0.2687711715698242f, 0.5216531753540039f, 0.1195623129606247f, 0.13672178983688354f, -0.11665651202201843f, -0.12065958976745605f, -0.1651592254638672f,
        -0.11785838007926941f, 0.23025867342948914f, -0.27260303497314453f, -0.5843105316162109f, 0.15096640586853027f, 0.0764782577753067f, 0.12086915969848633f,
        -0.21441233158111572f, 0.4120674133300781f, -0.22136303782463074f, 0.16626286506652832f, 0.5562324523925781f, 0.22799941897392273f, 0.4217414855957031f,
        0.06332918256521225f, 0.10036548972129822f, -0.12800240516662598f, 0.524752140045166f, -0.06530079990625381f, 0.07613521814346313f, 0.4829578399658203f,
        -0.2066720724105835f, 0.37589406967163086f, -0.08520495891571045f, -0.5470962524414062f, -0.04136413335800171f, 0.07944204658269882f,
        0.07440076023340225f, 0.12800845503807068f, 0.18773043155670166f, -0.25175726413726807f, 0.16903281211853027f, 0.40390515327453613f, 0.17806577682495117f,
        0.1419924646615982f, 0.430279016494751f, 0.0730820894241333f, 0.08360111713409424f, 0.3136415481567383f, 0.26016783714294434f, 0.05366674065589905f,
        -0.1095125675201416f, 0.5657272338867188f, 0.47479820251464844f, 0.24481701850891113f, 0.5373139381408691f, 0.2713773250579834f, -0.3577113151550293f,
        -0.3453998565673828f, 0.348669171333313f, -0.3311011791229248f, -0.44935500621795654f, 0.4792776107788086f, 0.3469885587692261f, -0.42234230041503906f,
        0.061429817229509354f, 0.4798698425292969f, 0.4571380615234375f, -0.19868898391723633f, 0.1474015861749649f, 0.011228948831558228f, 0.5897297859191895f,
        -0.4805877208709717f, -0.04750804603099823f, -0.35540294647216797f, 0.08602049946784973f, 0.4670743942260742f, 0.7380905151367188f, 0.11307299137115479f,
        -0.4366922378540039f, -0.03989333659410477f, 0.24200579524040222f, 0.019866645336151123f, -0.05359774827957153f, 0.3928098678588867f,
        -0.02843266725540161f, 0.6945419311523438f, -0.2017841339111328f, 0.487154483795166f, 0.9513702392578125f, 0.29308027029037476f, -0.6753196716308594f,
        -0.7516937255859375f, 0.32779788970947266f, 0.17924511432647705f, 0.6545131206512451f, 0.6691417694091797f, -0.18569332361221313f, -0.5621452331542969f,
        0.6424713134765625f, 0.5797119140625f, 0.6204032897949219f, 0.23674249649047852f, -0.09522445499897003f, 0.6935272216796875f, -0.014642611145973206f,
        -0.03791606426239014f, -0.33539628982543945f, -0.46245574951171875f, -0.11380016803741455f, 0.033883243799209595f, -0.23852387070655823f,
        -0.14328309893608093f, -0.2812456488609314f, -0.14591920375823975f, 0.36131906509399414f, 0.22288751602172852f, 0.06949453055858612f,
        0.09260541200637817f, 0.24962696433067322f, 0.05771958827972412f, 0.29703521728515625f, 0.08979743719100952f, 0.5573301315307617f, -0.5364007949829102f,
        -0.28522753715515137f, 0.013692647218704224f, 0.12964481115341187f, -0.16060566902160645f, 0.24487590789794922f, -0.1318955421447754f,
        0.13736005127429962f, -0.19165518879890442f, -0.5988407135009766f, -0.07555139064788818f, -0.12777027487754822f, 0.6502552032470703f,
        -0.31200146675109863f, 0.2313644289970398f, -0.13358443975448608f, 0.04877054691314697f, -0.18844124674797058f, 0.35041356086730957f,
        0.06880044937133789f, 0.17277801036834717f, -0.23554706573486328f, 0.6667137145996094f, 0.3693981170654297f, 0.23217353224754333f, -0.13571390509605408f,
        -0.08688163757324219f, -0.11378256231546402f, -0.05688504874706268f, -0.46852684020996094f, 0.2718580365180969f, 0.1955341100692749f,
        0.30518972873687744f, 0.10120588541030884f, -0.10056667029857635f, -0.018863312900066376f, -0.4694690704345703f, -0.013429969549179077f,
        0.24515044689178467f, 0.16560746729373932f, 0.5487403869628906f, 0.11098000407218933f, -0.0735851526260376f, 0.570587158203125f, 0.823089599609375f,
        -0.2941274642944336f, -0.10342895984649658f, -0.18265953660011292f, -0.009506523609161377f, 0.24918997287750244f, -0.31662988662719727f,
        -0.14391010999679565f, -0.2394922971725464f, 0.006232231855392456f, -0.06673678755760193f, 0.1439702808856964f, -0.47370386123657227f,
        -0.553227424621582f, -0.09386520087718964f, 0.3289494514465332f, 0.42947959899902344f, 0.005394693464040756f, 0.000249326229095459f, 0.12090480327606201f,
        0.3125278949737549f, 0.4348945617675781f, 4.41229248046875f, -0.06630316376686096f, 0.2644343972206116f, 0.44881319999694824f, -0.028876684606075287f,
        0.3977055549621582f, 0.19714555144309998f, -0.11797475814819336f, -0.004427820444107056f, 0.30222320556640625f, -0.1618201732635498f,
        -0.33331871032714844f, -0.1785638928413391f, 0.4960497319698334f, -0.04692494869232178f, -0.1797196865081787f, 0.07531329989433289f, 0.2536940574645996f,
        0.19402045011520386f, -0.2613224983215332f, -0.507598876953125f, 0.5046590566635132f, 0.25031328201293945f, -0.36165785789489746f, -0.0804600715637207f,
        0.6107559204101562f, 0.43488216400146484f, 0.5011863708496094f, 0.21435296535491943f, 0.25811493396759033f, 0.19697409868240356f, 0.21587252616882324f,
        -0.20930910110473633f, 0.20472025871276855f, -0.017730947583913803f, 0.2665634751319885f, 0.34143543243408203f, -0.29198503494262695f,
        0.14159345626831055f, 0.16976594924926758f, -0.24041622877120972f, 0.19643890857696533f, 0.18654680252075195f, 0.4027690887451172f, 0.17579281330108643f,
        -0.026663795113563538f, -0.0024576336145401f, 0.5350379943847656f, -0.07010854780673981f, -0.12165844440460205f, 0.12070822715759277f,
        -0.16678982973098755f, -0.18827438354492188f, 0.09550559520721436f, 0.5410079956054688f, 0.5095367431640625f, 0.23511013388633728f, 0.14226585626602173f,
        -0.39952898025512695f, -0.1851760894060135f, -0.16286814212799072f, -0.3411569595336914f, 0.32238566875457764f, -0.0007499493658542633f,
        -0.48739874362945557f, 0.34562015533447266f, 0.4751720428466797f, 0.5855426788330078f, 0.47747817635536194f, -0.2993730902671814f, 0.5547137260437012f,
        0.28569746017456055f, 0.10906652361154556f, -0.7275772094726562f, -0.10046738386154175f, -0.21497580409049988f, -0.29889869689941406f,
        -0.3723115921020508f, -0.009327482432126999f, -0.07505844533443451f, 0.014936596155166626f, -0.2595057487487793f, 0.012371830642223358f,
        0.1881422996520996f, -0.4127695560455322f, 0.37952423095703125f, 0.6558408737182617f, -0.32189929485321045f, 0.3388023376464844f, 0.4015007019042969f,
        0.28922557830810547f, 0.10649783909320831f, 0.25808167457580566f, -0.13588789105415344f, -0.01394808292388916f, 0.2782914638519287f, -0.4165172576904297f,
        -3.80841064453125f, 0.18964242935180664f, -0.2197357416152954f, -0.30334043502807617f, 0.19257831573486328f, 0.15302571654319763f, 0.15377503633499146f,
        0.4673480987548828f, -0.6588287353515625f, 0.3945174217224121f, 0.12062329053878784f, 0.12847498059272766f, -0.17056512832641602f, 0.1395660638809204f,
        0.19724047183990479f, 0.24941205978393555f, 0.06542408466339111f, 0.5407905578613281f, -0.1975189447402954f, -0.16631805896759033f, 0.22685623168945312f,
        0.32556647062301636f, 0.4309558868408203f, -0.3652350902557373f, 0.014116466045379639f, 0.16184386610984802f, 0.03948691487312317f, -0.1211346834897995f,
        -0.039695024490356445f, 0.07308685779571533f, 0.09855961799621582f, -0.18117475509643555f, 0.5927181243896484f, 0.2941575050354004f, 0.23846924304962158f,
        0.5159835815429688f, 0.3149888515472412f, -0.12948447465896606f, 0.0846254825592041f, 0.18111643195152283f, 0.07321619987487793f, 0.09212034195661545f,
        0.32189464569091797f, 0.5345916748046875f, -0.1291743814945221f, 0.28500884771347046f, 0.4014110565185547f, -0.13860464096069336f, -0.42892998456954956f,
        0.053245097398757935f, 0.37330126762390137f, 0.3198409080505371f, -0.09072775393724442f, 0.2812337875366211f, 0.5160999298095703f, 0.23632147908210754f,
        0.24299860000610352f, 0.33294248580932617f, -0.12973004579544067f, 0.2745506763458252f, -0.177057147026062f, 0.34788966178894043f, 0.13968408107757568f,
        0.09546241164207458f, -0.2546902298927307f, 0.05459490418434143f, 0.3341691493988037f, -0.0733904242515564f, 0.47107505798339844f, -0.24097204208374023f,
        0.1199001893401146f, 0.02550874650478363f, 0.38491177558898926f, -0.47585731744766235f, 0.2890276312828064f, 0.12301754951477051f, -0.39350128173828125f,
        0.1369415521621704f, 0.6089630126953125f, 0.30422019958496094f, 0.008393317461013794f, 0.20327943563461304f, -0.5396957397460938f, 0.5219254493713379f,
        2.1754302978515625f, 0.2431035041809082f, 2.1421661376953125f, 0.30678462982177734f, -0.12712536752223969f, 0.2702474594116211f, -0.06497690081596375f,
        0.3353118896484375f, 0.2025918960571289f, 0.5018405914306641f, -0.12535378336906433f, 0.304744154214859f, -0.4438133239746094f, 0.1323385238647461f,
        -0.0331556610763073f, -0.4864540100097656f, 0.19876164197921753f, -0.8323459625244141f, -0.12086188793182373f, 0.23592877388000488f,
        0.046369217336177826f, 0.42406606674194336f, 0.2882566452026367f, 0.4054187536239624f, -0.16171666979789734f, -0.08917970955371857f, 0.0830727219581604f,
        0.05238346755504608f, -0.022979795932769775f, 0.03473946452140808f, -0.1306568682193756f, 0.3847780227661133f, 0.21573632955551147f, 0.2353387176990509f,
        0.1677708923816681f, 0.3768329620361328f, -0.1285761594772339f, 4.5460205078125f, 0.4414329528808594f, -0.21170389652252197f, -0.0036725737154483795f,
        0.19332671165466309f, 0.6124951839447021f, 0.7336006164550781f, -0.526214599609375f, -0.36821556091308594f, -0.08081982284784317f, 0.16092944145202637f,
        -0.14703470468521118f, 0.3288337290287018f, -0.0671909898519516f, 0.07612776756286621f, 0.04888370633125305f, -0.07860139012336731f, 0.10552829504013062f,
        0.15144801139831543f, 0.13594436645507812f, -0.1816648244857788f, -0.3002445101737976f, 0.6317329406738281f, 0.06497293710708618f, 0.07822516560554504f,
        -0.04976940155029297f, -0.2947622537612915f, 0.42537403106689453f, -0.011703613214194775f, 0.2509944438934326f, -0.06058981642127037f, 5.2864990234375f,
        -0.021076202392578125f, -0.042911142110824585f, -0.23166412115097046f, -0.4726858139038086f, 0.18450456857681274f, -0.23915709555149078f,
        0.12817111611366272f, -0.46313953399658203f, 0.14935946464538574f, 0.17296548187732697f, -0.10982483625411987f, -0.0328361839056015f,
        0.24515235424041748f, 0.16088783740997314f, 0.06592860817909241f, -0.1965874433517456f, -0.13309836387634277f, -0.07501828670501709f,
        -0.22989428043365479f, -0.11766546964645386f, 0.5713901519775391f, 0.24230670928955078f, 0.4787154197692871f, 0.1493518054485321f, -0.07022184133529663f,
        -0.28978586196899414f, 0.5235023498535156f, 0.1286548376083374f, 0.07207280397415161f, 0.43700504302978516f, 0.08260849118232727f, -0.47586584091186523f,
        0.2593594789505005f, -0.2415151596069336f, -0.08712369203567505f, 0.3357248306274414f, -0.04695922136306763f, -0.003928631544113159f,
        0.051069408655166626f, 0.2725105285644531f, 0.40708065032958984f, 0.08131939172744751f, -0.0004536956548690796f, -0.3460259437561035f,
        0.15716946125030518f, -0.17775702476501465f, 0.24414682388305664f, -0.009145110845565796f, 0.01075822114944458f, -0.139506995677948f,
        0.33933162689208984f, 0.7788848876953125f, 1.0451393127441406f, 0.6160564422607422f, 0.1908702850341797f, -0.07777714729309082f, 0.06223928928375244f,
        0.7796478271484375f, -0.05027353763580322f, 0.1575016975402832f, 0.1773061752319336f, -0.0051665883511304855f, 0.32943224906921387f, 0.5890083312988281f,
        0.18775200843811035f, -0.11093059182167053f, 0.0961659848690033f, 0.834136962890625f, -0.19726219773292542f, -0.4463539123535156f, 0.04503452777862549f,
        0.3012208938598633f, -0.18203765153884888f, 0.015116162598133087f, 0.06849271059036255f, -0.1706831455230713f, -0.19199156761169434f,
        -0.45730578899383545f, -0.3191685676574707f, 0.10347956418991089f, -0.4093894958496094f, 0.024946987628936768f, 0.09663379192352295f,
        -0.0926671177148819f, -0.27153003215789795f, -0.03945004940032959f, -0.010053455829620361f, 0.017528608441352844f, 0.21634721755981445f,
        -0.006661977618932724f, -0.09938253462314606f, 0.35866475105285645f, -0.36626702547073364f, 0.17892146110534668f, -0.016031354665756226f,
        0.5111522674560547f, 0.6398804187774658f, -0.13153648376464844f, 0.5075316429138184f, -0.2253853976726532f, 0.21571892499923706f, 0.21523034572601318f,
        0.16129571199417114f, -0.017935559153556824f, 0.3400092124938965f, 0.16615986824035645f, -0.6552600860595703f, 0.3381650447845459f, -0.3385718762874603f,
        0.6094970703125f, 0.009996533393859863f, -0.09696429967880249f, 0.02345132827758789f, 0.06207479536533356f
    ];

    private static float[] Vector2 =>
    [
        0.13004936277866364f, 0.4297241270542145f, 0.14029307663440704f, 0.3117102086544037f, -0.07381683588027954f, -0.07958175987005234f, 0.49644288420677185f,
        -0.26625120639801025f, 0.18565750122070312f, 0.2699015736579895f, -0.3150564432144165f, 0.27458542585372925f, -0.2434309422969818f, -0.04715972766280174f,
        -0.5089501738548279f, -0.39154914021492004f, 0.5021093487739563f, 0.24324020743370056f, -0.42775022983551025f, 0.041338901966810226f,
        -0.06589552760124207f, 0.04618438705801964f, 0.1971658319234848f, 0.05367717891931534f, 0.2982177734375f, 0.05406860262155533f, -0.12739592790603638f,
        -0.04409448802471161f, -0.4063684046268463f, 0.33500245213508606f, 0.20078231394290924f, -0.13494782149791718f, -0.3194934129714966f, 0.4976121485233307f,
        -0.7919726371765137f, 0.29633423686027527f, -0.0684732049703598f, -0.05185447633266449f, 0.05379221960902214f, 0.3640033006668091f, 0.16454681754112244f,
        0.27787110209465027f, -0.015831680968403816f, -0.02177482657134533f, 0.1450640857219696f, 0.15692749619483948f, 0.0020017814822494984f,
        -0.47017332911491394f, 0.024961089715361595f, 0.009582748636603355f, -0.2815515100955963f, 0.17024993896484375f, -0.5069286823272705f,
        -0.18173980712890625f, -0.012595214881002903f, 0.09752418845891953f, -0.1762963831424713f, 0.20084305107593536f, -0.06730293482542038f,
        0.3123144507408142f, 0.010116677731275558f, 0.2978179156780243f, -0.04450523480772972f, -0.2080630511045456f, -0.29302138090133667f, 0.19446974992752075f,
        0.048525430262088776f, 0.23003502190113068f, -0.15965591371059418f, 0.5710400342941284f, 0.14655959606170654f, 0.1618601232767105f, 0.5482519268989563f,
        0.26045286655426025f, 0.263916015625f, 0.05234260484576225f, 0.18466247618198395f, -0.2585693299770355f, 0.3902691602706909f, -0.07458175718784332f,
        0.0491805262863636f, 0.07990745455026627f, -0.01068954449146986f, 0.48760560154914856f, 0.3724707067012787f, 0.5526318550109863f, 0.12869201600551605f,
        0.023037947714328766f, 0.29638671875f, 0.3814099133014679f, -0.2895141541957855f, 0.1520276516675949f, -0.2276906967163086f, 0.16979674994945526f,
        -0.20511047542095184f, 0.5864377021789551f, 0.4670483469963074f, -0.18610168993473053f, -0.0911267101764679f, -0.4119979739189148f, 0.16466541588306427f,
        -0.24265868961811066f, -0.13836491107940674f, 0.14126616716384888f, 0.04160340130329132f, -0.34959959983825684f, 0.17959660291671753f,
        0.28353118896484375f, 0.539563000202179f, 0.42515382170677185f, -0.07431068271398544f, -0.10898345708847046f, -0.03673553466796875f, 0.2394174188375473f,
        0.43983641266822815f, -0.0693441778421402f, 0.0424845889210701f, 0.05292191356420517f, -0.23418563604354858f, -0.9228906035423279f, 0.46814942359924316f,
        0.14685295522212982f, -0.3584521412849426f, -0.46930205821990967f, 0.42293059825897217f, -0.12437446415424347f, 0.6115576028823853f, -0.1288050413131714f,
        0.6536571979522705f, 0.32020628452301025f, -0.35900261998176575f, -0.05724773555994034f, 0.5019915699958801f, 0.4823046922683716f, 0.3171769678592682f,
        0.09785468876361847f, 0.2626644968986511f, 0.2356555163860321f, -0.13228240609169006f, -0.6696581840515137f, 0.0016715622041374445f,
        -0.26253142952919006f, 0.25082823634147644f, 0.5938256978988647f, -0.043012913316488266f, 0.05433618649840355f, -0.00942276045680046f,
        0.5826122760772705f, 0.21598266065120697f, 0.35044920444488525f, -0.13645844161510468f, -0.07611420005559921f, -0.027253780514001846f,
        0.17003174126148224f, -0.1850012242794037f, 0.40348997712135315f, 0.7110119462013245f, 0.0429576113820076f, 0.5340930223464966f, -0.23856239020824432f,
        0.667309582233429f, 0.29660889506340027f, 0.07339790463447571f, -0.42047178745269775f, -0.17041580379009247f, 0.19370399415493011f, -0.0705450028181076f,
        0.4421118199825287f, 0.48512694239616394f, -0.27343201637268066f, -0.4392944276332855f, 0.08981170505285263f, 0.33638426661491394f,
        -0.047511328011751175f, 0.3850415050983429f, 0.003099727677181363f, -0.03070859983563423f, 0.20876282453536987f, -0.16398650407791138f,
        0.0124053955078125f, 0.04782967269420624f, 0.24286483228206635f, 0.052783966064453125f, 0.4845291078090668f, 0.5612329244613647f, 0.1765161156654358f,
        0.2146379053592682f, -0.034403420984745026f, -0.2848767042160034f, -0.01901153475046158f, 0.06098693981766701f, -0.10051605105400085f,
        0.6783667206764221f, -0.24233493208885193f, 0.10672897100448608f, 0.1441558450460434f, -0.17762954533100128f, 0.7371532917022705f, -0.1582918018102646f,
        0.232513427734375f, -0.0009263610700145364f, 0.247935950756073f, -0.35853272676467896f, 0.11470794677734375f, 0.38023436069488525f, -0.6933642625808716f,
        0.2701597511768341f, 0.08223319798707962f, -0.046813927590847015f, 0.10010483115911484f, -0.061490174382925034f, 0.11503265053033829f,
        0.5537652373313904f, -0.08019372820854187f, -0.2814355492591858f, 0.08793991059064865f, -0.2123342901468277f, 0.33674561977386475f, 0.5238671898841858f,
        -0.00226140976883471f, -0.2021307349205017f, 0.18811428546905518f, -0.13057586550712585f, -0.13216964900493622f, 0.3037353456020355f,
        0.06527633965015411f, -0.17299681901931763f, -0.8040624856948853f, 0.09468704462051392f, 0.24891479313373566f, 0.0421239472925663f, 0.10383392125368118f,
        0.24551880359649658f, -0.3461126685142517f, 0.4950610399246216f, 0.6422412395477295f, 0.2248345911502838f, 0.35637450218200684f, 0.20258453488349915f,
        -0.1396344006061554f, 0.08070465177297592f, 0.1602770984172821f, -0.4110791087150574f, 0.26444703340530396f, 0.5233349800109863f, -0.15119141340255737f,
        0.28627195954322815f, -0.031078491359949112f, -0.8915625214576721f, 0.021961670368909836f, -0.041693344712257385f, 0.09245208650827408f,
        0.1041635125875473f, 0.11551828682422638f, -0.45832520723342896f, 0.20880645513534546f, 0.5018163919448853f, 0.09306828677654266f, 0.10348289459943771f,
        0.46119385957717896f, 0.14375004172325134f, 0.3725408911705017f, 0.25773316621780396f, 0.3792431652545929f, -0.12950843572616577f, 0.0574936680495739f,
        0.5367773175239563f, 0.45610350370407104f, 0.056842535734176636f, 0.43159806728363037f, 0.21396301686763763f, -0.3636767566204071f, -0.19751793146133423f,
        0.6045446991920471f, -0.2553747594356537f, -0.4778686463832855f, 0.2949978709220886f, -0.0369623564183712f, -0.4321215748786926f, -0.004182548727840185f,
        0.5542431473731995f, 0.15986496210098267f, -0.21222549676895142f, 0.15302476286888123f, 0.2764221131801605f, 0.44437071681022644f, -0.20808501541614532f,
        -0.0374922938644886f, -0.26304107904434204f, 0.15791819989681244f, 0.29231080412864685f, 0.602294921875f, -0.06578466296195984f, -0.2239968180656433f,
        0.30941450595855713f, 0.20904922485351562f, 0.12343725562095642f, -0.1680871546268463f, 0.19073447585105896f, -0.17053017020225525f, 0.6497998237609863f,
        -0.4247119128704071f, 0.5322094559669495f, 0.5981335639953613f, 0.2690504491329193f, -0.5576953291893005f, -1.111484408378601f, 0.3722322881221771f,
        0.03813306987285614f, 0.5436059832572937f, 0.21595421433448792f, -0.031949080526828766f, -0.4018310606479645f, 0.42970702052116394f, 0.5407519340515137f,
        0.2410629242658615f, 0.14524100720882416f, -0.043616630136966705f, 0.8606250286102295f, 0.0318264402449131f, -0.2731921374797821f, -0.1536828577518463f,
        -0.4174121022224426f, -0.06635833531618118f, 0.3560815453529358f, -0.32845643162727356f, 0.07291687279939651f, -0.2604386806488037f,
        -0.10849662870168686f, 0.2111465483903885f, 0.5559771656990051f, 0.27229034900665283f, -0.09060869365930557f, 0.24895621836185455f, -0.128291517496109f,
        0.34312498569488525f, -0.08974544703960419f, 0.566174328327179f, -0.41370728611946106f, -0.35120972990989685f, 0.3008366823196411f,
        0.0073123169131577015f, 0.01677757315337658f, 0.2654370069503784f, -0.30050233006477356f, 0.30552855134010315f, -0.22380447387695312f,
        -0.03454536572098732f, 0.1904478520154953f, 0.06433513760566711f, 0.31111785769462585f, -0.32732972502708435f, -0.06273562461137772f, -0.157551109790802f,
        0.17928874492645264f, -0.19615569710731506f, 0.6021057367324829f, 0.033119965344667435f, 0.10743334144353867f, -0.2854144275188446f, 0.4797094762325287f,
        0.30903199315071106f, 0.45521727204322815f, 0.18240828812122345f, 0.12367072701454163f, 0.02879868447780609f, 0.011693878099322319f, -0.4947241246700287f,
        0.17513839900493622f, 0.08583007752895355f, 0.4654949903488159f, 0.08862259238958359f, -0.03625897318124771f, -0.009972200728952885f,
        -0.7134423851966858f, -0.08201146870851517f, 0.2922845482826233f, 0.24239562451839447f, 0.4769970774650574f, -0.15491913259029388f, 0.17544464766979218f,
        0.5068163871765137f, 0.5475243926048279f, -0.23502197861671448f, -0.03520652651786804f, -0.10292663425207138f, -0.12088155746459961f,
        -0.08344520628452301f, -0.07288943976163864f, 0.03466327488422394f, -0.1415887475013733f, -0.1795220524072647f, -0.27394407987594604f, 0.175755113363266f,
        -0.15306365489959717f, -0.3107617199420929f, -0.1399800181388855f, 0.4250186085700989f, 0.13368690013885498f, -0.009905090555548668f,
        0.03442218899726868f, 0.25747984647750854f, 0.10512056201696396f, 0.3062414526939392f, 4.387773513793945f, -0.19145850837230682f, 0.2598351240158081f,
        0.5396429300308228f, 0.06471487134695053f, 0.3784753382205963f, -0.02930217795073986f, -0.13270500302314758f, -0.022182617336511612f,
        0.21027222275733948f, -0.06559501588344574f, -0.20126831531524658f, -0.2838728427886963f, 0.268067866563797f, -0.019517220556735992f,
        -0.07517166435718536f, 0.22638244926929474f, 0.06558199971914291f, 0.2132067084312439f, -0.022311324253678322f, -0.4654003977775574f, 0.4026605188846588f,
        0.07629470527172089f, -0.014941406436264515f, -0.08239241689443588f, 0.43693238496780396f, 0.3483419716358185f, 0.18319137394428253f, 0.38922119140625f,
        0.1103883758187294f, 0.13617008924484253f, 0.22181066870689392f, -0.08012153953313828f, 0.2104302942752838f, -0.3536175489425659f, 0.17789150774478912f,
        0.1855599582195282f, -0.11558616906404495f, -0.07637817412614822f, 0.3907446265220642f, -0.31724852323532104f, -0.05008377134799957f,
        0.46971678733825684f, 0.5529345870018005f, 0.16348594427108765f, -0.00508739473298192f, -0.40045166015625f, 0.519519031047821f, 0.03505857288837433f,
        0.05345504730939865f, 0.05200177803635597f, -0.28374725580215454f, -0.10775306820869446f, 0.0016325878677889705f, 0.1364092379808426f,
        0.5188574194908142f, 0.31802916526794434f, 0.6449170112609863f, -0.3473571836948395f, -0.18012572824954987f, -0.18163615465164185f, -0.1164480596780777f,
        0.2973608374595642f, -0.23583923280239105f, -0.4907028079032898f, 0.15003052353858948f, 0.5060327053070068f, 0.6000146269798279f, 0.4242602586746216f,
        -0.3273056149482727f, 0.6519018411636353f, 0.3252197206020355f, 0.24095703661441803f, -0.7179492115974426f, -0.0883573368191719f, -0.10058419406414032f,
        -0.5126281976699829f, -0.09105224907398224f, -0.22818267345428467f, 0.08345939964056015f, 0.06144513934850693f, -0.1986352503299713f,
        -0.16254058480262756f, 0.2341143786907196f, -0.3276193141937256f, 0.5201464891433716f, 0.5550079345703125f, -0.454353928565979f, 0.27381956577301025f,
        0.251341849565506f, 0.35069334506988525f, 0.0955391675233841f, 0.28791505098342896f, 0.10405199974775314f, 0.2529257535934448f, 0.2769168019294739f,
        -0.005870666354894638f, -3.897890567779541f, 0.25926026701927185f, 0.05720745027065277f, -0.3392700254917145f, 0.1330946385860443f, 0.3601202368736267f,
        -0.005457496736198664f, 0.573681652545929f, -0.670483410358429f, 0.15858307480812073f, 0.0381394661962986f, 0.28364866971969604f, -0.15219207108020782f,
        0.31879547238349915f, -0.11190910637378693f, 0.30411621928215027f, 0.03705398738384247f, 0.23790527880191803f, -0.06706111133098602f,
        -0.14260421693325043f, 0.1412666290998459f, 0.057696837931871414f, 0.3002075254917145f, -0.3502329885959625f, 0.3183862268924713f, 0.38758790493011475f,
        -0.3176353871822357f, -0.1994483917951584f, -0.0047987173311412334f, 0.05226119980216026f, 0.17825622856616974f, 0.04828247055411339f,
        0.39225587248802185f, -0.00545286200940609f, 0.14104917645454407f, 0.6539599895477295f, 0.3312947154045105f, 0.09927912056446075f, 0.03848749026656151f,
        0.27910521626472473f, 0.10525757074356079f, 0.23137779533863068f, 0.4109179675579071f, 0.3829101622104645f, 0.1868864893913269f, 0.33855804800987244f,
        0.31755828857421875f, 0.2588835656642914f, -0.1954444944858551f, 0.14337128400802612f, 0.2162335216999054f, 0.4211474657058716f, -0.4975244104862213f,
        0.2930285632610321f, 0.31901609897613525f, 0.3205874562263489f, -0.0003406143223401159f, 0.05211467668414116f, 0.12363609671592712f, 0.12317872792482376f,
        -0.2720526158809662f, 0.30747589468955994f, 0.18313170969486237f, 0.04621032625436783f, 0.030085982754826546f, -0.1578587293624878f, 0.2852635085582733f,
        -0.05473007261753082f, 0.2350979596376419f, -0.2637915015220642f, 0.24922241270542145f, 0.24854187667369843f, 0.24138152599334717f, -0.2289111316204071f,
        -0.026599163189530373f, 0.3628198206424713f, -0.26140472292900085f, -0.0015947723295539618f, 0.5677343606948853f, 0.1556474268436432f,
        0.16885574162006378f, 0.48640623688697815f, -0.49229004979133606f, 0.18722331523895264f, 2.3437891006469727f, 0.3135937452316284f, 2.189746141433716f,
        0.30574795603752136f, -0.197764590382576f, 0.31990477442741394f, 0.11521179229021072f, 0.16085441410541534f, 0.01860244758427143f, 0.17673736810684204f,
        0.12399095296859741f, 0.18609954416751862f, -0.29549071192741394f, 0.22790314257144928f, -0.3078497350215912f, -0.3426489233970642f, 0.20670028030872345f,
        -0.9915136694908142f, -0.13466857373714447f, -0.08772353827953339f, 0.13441459834575653f, 0.37076401710510254f, 0.20269866287708282f, 0.2528340816497803f,
        -0.09601593017578125f, -0.03571895509958267f, -0.14285065233707428f, 0.11855529993772507f, 0.020663194358348846f, -0.030170859768986702f,
        -0.1619023084640503f, 0.3485879600048065f, 0.38318848609924316f, -0.007417888846248388f, 0.023795776069164276f, 0.3937451243400574f,
        -0.21612396836280823f, 4.578281402587891f, -0.0037953616119921207f, 0.05105220898985863f, 0.09547393769025803f, 0.28485107421875f, 0.3079194128513336f,
        0.6161572337150574f, -0.5044335722923279f, -0.23694488406181335f, 0.2807180881500244f, 0.0591466911137104f, -0.2968124449253082f, 0.40354737639427185f,
        -0.11341766268014908f, 0.16466613113880157f, 0.28663453459739685f, 0.07947349548339844f, -0.06793441623449326f, 0.2057969719171524f, 0.07696887850761414f,
        0.4267578125f, -0.03662143647670746f, 0.5250805616378784f, -0.008626861497759819f, -0.1367434710264206f, 0.010828323662281036f, -0.3181823790073395f,
        0.320089727640152f, -0.012104492634534836f, 0.5141577124595642f, -0.10688433796167374f, 5.361015796661377f, 0.3774639964103699f, 0.057402439415454865f,
        -0.035071104764938354f, -0.21543151140213013f, 0.27234020829200745f, -0.2824328541755676f, -0.21881622076034546f, -0.3709551990032196f,
        0.054383937269449234f, 0.1621844470500946f, -0.1093127429485321f, -0.045165881514549255f, 0.014125023037195206f, 0.12727993726730347f,
        0.09958768635988235f, -0.15506333112716675f, 0.007707290817052126f, -0.1426861584186554f, -0.2696276903152466f, -0.03123645856976509f,
        0.15271545946598053f, 0.08066543936729431f, -0.033773843199014664f, 0.42693892121315f, 0.0504109188914299f, -0.19427764415740967f, 0.11983248591423035f,
        0.028964785858988762f, -0.031806182116270065f, 0.33408936858177185f, 0.2943750023841858f, -0.019622039049863815f, 0.09427303075790405f,
        -0.037819214165210724f, 0.031827088445425034f, 0.17850738763809204f, 0.12330963462591171f, 0.31262147426605225f, -0.15524886548519135f,
        0.3373364210128784f, 0.1713554412126541f, 0.01864219643175602f, 0.025014495477080345f, -0.117446668446064f, 0.020533274859189987f, -0.18519729375839233f,
        0.2550015151500702f, 0.109202079474926f, 0.1324794590473175f, -0.18250694870948792f, 0.22657226026058197f, 0.7202734351158142f, 0.5555566549301147f,
        0.3049963414669037f, 0.3309326171875f, -0.0393625870347023f, 0.21467262506484985f, 0.43003419041633606f, 0.22019775211811066f, 0.2810742259025574f,
        0.18106964230537415f, 0.01548721268773079f, 0.375131219625473f, 0.6922851800918579f, 0.14404800534248352f, 0.13661658763885498f, 0.0551217645406723f,
        0.7218652367591858f, -0.13048484921455383f, -0.25265443325042725f, -0.06120327115058899f, 0.08435272425413132f, -0.04759795963764191f,
        -0.05841277912259102f, -0.2210504114627838f, -0.301034539937973f, -0.44509032368659973f, -0.14129523932933807f, -0.17858734726905823f,
        0.09358871728181839f, -0.08902553468942642f, 0.01724778674542904f, -0.0067562866024672985f, -0.11558642983436584f, -0.260682076215744f,
        -0.5220129489898682f, 0.05793702229857445f, 0.18178848922252655f, 0.1496656835079193f, -0.23060119152069092f, 0.08460605889558792f, 0.1616632044315338f,
        0.0200917050242424f, 0.3466528356075287f, -0.006882190704345703f, 0.4706338047981262f, 0.46332764625549316f, 0.03791441023349762f, 0.5466833710670471f,
        -0.032548677176237106f, 0.08397018164396286f, 0.1929180920124054f, -0.00252361292950809f, 0.12893646955490112f, 0.1647239625453949f,
        -0.043830908834934235f, -0.2504025399684906f, 0.3420040011405945f, -0.10061424225568771f, 0.5895898342132568f, 0.2457064837217331f, -0.15506866574287415f,
        -0.05577230080962181f, 0.1625908613204956f
    ];
}