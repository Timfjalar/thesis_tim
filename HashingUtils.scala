import com.tim.thesis.featureIdParser.shared.LazyLogger
import com.tim.thesis.featureIdParser.utils.CoordinateUtils._
import net.jpountz.xxhash.XXHashFactory
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.nio.{ByteBuffer, LongBuffer}
import java.util.zip.GZIPOutputStream
import scala.annotation.tailrec
import scala.math.BigDecimal.RoundingMode


object HashingUtils extends LazyLogger {

  case class RoadSegment(name: String, coordinates: Seq[Coordinate], tags: Map[String, String])


  val ELEVATION_TAGS: Set[String] = Set("level", "layer")

  private val LENGTH_INTERVALS_32 = Seq(0, 473, 731, 1010, 1332, 1702, 2128, 2654, 3235, 3887, 4610, 5464, 6415, 7544, 8885, 10530, 12389, 14622, 17350, 20607, 24645, 29690, 36010, 44254, 54611, 68453, 86634, 113511, 151506, 213727, 326512, 598326, Integer.MAX_VALUE)
  private val LENGTH_INTERVALS_2048 = Seq(0, 87, 114, 136, 155, 173, 189, 205, 219, 234, 248, 261, 274, 287, 299, 311, 323, 334, 346, 357, 368, 379, 389, 400, 410, 420, 430, 440, 450, 459, 469, 479, 488, 497, 507, 516, 525, 534, 543, 551, 559, 568, 576, 585, 593, 602, 610, 618, 626, 634, 642, 650, 657, 665, 673, 681, 688, 696, 704, 711, 719, 726, 734, 741, 749, 756, 763, 771, 778, 785, 792, 800, 807, 814, 821, 828, 835, 842, 849, 856, 863, 870, 877, 884, 891, 898, 903, 909, 915, 922, 928, 935, 942, 948, 955, 962, 969, 975, 982, 989, 996, 1003, 1009, 1016, 1023, 1030, 1037, 1044, 1051, 1058, 1065, 1072, 1079, 1086, 1093, 1100, 1107, 1114, 1121, 1128, 1135, 1142, 1149, 1157, 1164, 1171, 1178, 1185, 1192, 1199, 1206, 1213, 1220, 1227, 1234, 1242, 1249, 1256, 1263, 1270, 1277, 1284, 1291, 1298, 1306, 1313, 1320, 1327, 1334, 1341, 1348, 1356, 1363, 1370, 1377, 1385, 1392, 1399, 1406, 1414, 1421, 1428, 1436, 1443, 1450, 1458, 1465, 1472, 1480, 1487, 1495, 1502, 1509, 1517, 1524, 1532, 1539, 1547, 1554, 1562, 1570, 1577, 1585, 1592, 1600, 1607, 1615, 1622, 1630, 1638, 1645, 1653, 1660, 1668, 1676, 1683, 1691, 1699, 1707, 1714, 1722, 1730, 1738, 1746, 1753, 1761, 1769, 1777, 1785, 1793, 1801, 1808, 1816, 1824, 1832, 1840, 1848, 1856, 1864, 1872, 1880, 1889, 1897, 1905, 1913, 1921, 1929, 1938, 1946, 1954, 1962, 1971, 1979, 1987, 1996, 2004, 2012, 2021, 2029, 2038, 2046, 2055, 2063, 2072, 2080, 2089, 2097, 2106, 2115, 2123, 2132, 2140, 2149, 2158, 2166, 2175, 2184, 2193, 2201, 2210, 2219, 2228, 2236, 2245, 2254, 2263, 2272, 2281, 2290, 2299, 2307, 2316, 2325, 2334, 2343, 2352, 2361, 2370, 2379, 2388, 2397, 2406, 2415, 2424, 2433, 2442, 2451, 2460, 2469, 2478, 2488, 2497, 2506, 2515, 2524, 2533, 2542, 2551, 2560, 2570, 2579, 2588, 2597, 2606, 2615, 2625, 2634, 2643, 2652, 2661, 2671, 2680, 2689, 2698, 2708, 2717, 2726, 2735, 2745, 2754, 2763, 2772, 2782, 2791, 2800, 2809, 2819, 2828, 2837, 2847, 2856, 2865, 2875, 2884, 2894, 2903, 2912, 2921, 2930, 2940, 2949, 2958, 2968, 2977, 2987, 2996, 3005, 3015, 3024, 3033, 3043, 3052, 3062, 3071, 3080, 3090, 3099, 3109, 3118, 3127, 3137, 3146, 3156, 3165, 3175, 3184, 3194, 3203, 3213, 3222, 3232, 3241, 3251, 3260, 3270, 3279, 3289, 3298, 3308, 3317, 3327, 3336, 3346, 3356, 3365, 3375, 3384, 3394, 3403, 3413, 3423, 3432, 3442, 3452, 3461, 3471, 3480, 3490, 3500, 3509, 3519, 3529, 3538, 3548, 3558, 3567, 3577, 3587, 3596, 3606, 3616, 3625, 3635, 3645, 3654, 3664, 3674, 3684, 3693, 3703, 3713, 3723, 3732, 3742, 3752, 3761, 3771, 3781, 3791, 3800, 3810, 3820, 3830, 3840, 3849, 3859, 3869, 3879, 3889, 3899, 3909, 3918, 3928, 3938, 3948, 3958, 3968, 3978, 3988, 3997, 4007, 4017, 4027, 4037, 4047, 4057, 4067, 4077, 4087, 4097, 4107, 4117, 4127, 4137, 4147, 4157, 4167, 4177, 4188, 4198, 4208, 4218, 4228, 4238, 4248, 4258, 4269, 4279, 4289, 4299, 4309, 4319, 4330, 4340, 4350, 4360, 4370, 4381, 4391, 4401, 4411, 4421, 4432, 4442, 4452, 4462, 4473, 4483, 4493, 4504, 4514, 4524, 4534, 4545, 4555, 4565, 4575, 4584, 4593, 4603, 4612, 4621, 4630, 4639, 4648, 4658, 4667, 4676, 4686, 4695, 4705, 4714, 4724, 4733, 4743, 4752, 4762, 4772, 4781, 4791, 4801, 4810, 4820, 4830, 4839, 4849, 4859, 4869, 4878, 4888, 4898, 4908, 4917, 4927, 4937, 4947, 4957, 4967, 4977, 4986, 4996, 5006, 5015, 5025, 5035, 5045, 5055, 5065, 5075, 5084, 5094, 5104, 5114, 5124, 5134, 5145, 5155, 5165, 5175, 5185, 5195, 5205, 5215, 5225, 5236, 5246, 5256, 5266, 5276, 5287, 5297, 5307, 5317, 5328, 5338, 5348, 5359, 5369, 5379, 5390, 5400, 5410, 5421, 5431, 5442, 5452, 5462, 5473, 5483, 5494, 5504, 5515, 5525, 5536, 5546, 5557, 5567, 5578, 5588, 5599, 5609, 5620, 5631, 5641, 5652, 5663, 5673, 5684, 5695, 5705, 5716, 5727, 5737, 5748, 5759, 5770, 5780, 5791, 5802, 5813, 5824, 5834, 5845, 5856, 5867, 5878, 5889, 5900, 5911, 5921, 5932, 5943, 5954, 5965, 5976, 5987, 5998, 6009, 6020, 6031, 6042, 6053, 6064, 6075, 6086, 6097, 6108, 6119, 6130, 6142, 6153, 6164, 6175, 6186, 6198, 6209, 6220, 6231, 6243, 6254, 6265, 6277, 6288, 6299, 6311, 6322, 6334, 6345, 6356, 6368, 6380, 6391, 6402, 6414, 6425, 6437, 6449, 6460, 6472, 6483, 6495, 6507, 6518, 6530, 6542, 6553, 6565, 6577, 6589, 6600, 6612, 6624, 6636, 6648, 6660, 6672, 6683, 6695, 6707, 6719, 6731, 6743, 6755, 6767, 6779, 6791, 6803, 6816, 6828, 6840, 6852, 6864, 6876, 6888, 6900, 6913, 6925, 6937, 6949, 6962, 6974, 6986, 6998, 7011, 7023, 7036, 7048, 7060, 7073, 7085, 7098, 7110, 7123, 7135, 7148, 7160, 7173, 7185, 7198, 7210, 7223, 7236, 7248, 7261, 7274, 7286, 7299, 7312, 7324, 7337, 7350, 7363, 7376, 7389, 7401, 7414, 7427, 7440, 7453, 7466, 7479, 7492, 7505, 7518, 7531, 7544, 7557, 7570, 7583, 7596, 7609, 7622, 7635, 7648, 7662, 7675, 7688, 7701, 7714, 7728, 7741, 7754, 7768, 7781, 7795, 7808, 7821, 7835, 7848, 7862, 7875, 7889, 7902, 7916, 7929, 7943, 7957, 7970, 7984, 7997, 8011, 8025, 8039, 8052, 8066, 8080, 8094, 8107, 8121, 8135, 8149, 8163, 8177, 8191, 8205, 8219, 8233, 8247, 8261, 8275, 8289, 8303, 8317, 8332, 8346, 8360, 8374, 8388, 8403, 8417, 8431, 8446, 8460, 8475, 8489, 8503, 8518, 8532, 8547, 8561, 8576, 8590, 8605, 8619, 8634, 8649, 8663, 8678, 8693, 8708, 8722, 8737, 8752, 8767, 8782, 8797, 8811, 8826, 8841, 8856, 8871, 8886, 8901, 8916, 8931, 8946, 8961, 8976, 8991, 9007, 9022, 9037, 9052, 9067, 9083, 9098, 9113, 9128, 9144, 9159, 9174, 9189, 9205, 9220, 9236, 9251, 9267, 9282, 9298, 9314, 9329, 9345, 9361, 9376, 9392, 9408, 9424, 9440, 9455, 9471, 9487, 9503, 9519, 9535, 9551, 9567, 9583, 9599, 9615, 9631, 9647, 9664, 9680, 9696, 9712, 9728, 9744, 9761, 9777, 9793, 9810, 9826, 9842, 9859, 9875, 9891, 9908, 9924, 9941, 9957, 9973, 9990, 10006, 10023, 10039, 10056, 10072, 10089, 10106, 10122, 10139, 10156, 10172, 10189, 10206, 10223, 10240, 10257, 10274, 10291, 10308, 10325, 10342, 10359, 10377, 10394, 10411, 10428, 10446, 10463, 10480, 10498, 10515, 10533, 10550, 10568, 10585, 10603, 10621, 10638, 10656, 10674, 10691, 10709, 10727, 10745, 10763, 10781, 10799, 10816, 10834, 10852, 10871, 10889, 10907, 10925, 10943, 10961, 10979, 10997, 11016, 11034, 11052, 11070, 11089, 11107, 11126, 11144, 11163, 11181, 11200, 11218, 11237, 11255, 11274, 11293, 11311, 11330, 11349, 11368, 11387, 11406, 11425, 11444, 11463, 11482, 11501, 11520, 11540, 11559, 11578, 11597, 11617, 11636, 11656, 11675, 11695, 11714, 11734, 11754, 11773, 11793, 11813, 11833, 11853, 11873, 11892, 11912, 11932, 11952, 11972, 11993, 12013, 12033, 12053, 12073, 12094, 12114, 12135, 12155, 12176, 12196, 12217, 12237, 12258, 12279, 12300, 12321, 12342, 12363, 12384, 12405, 12426, 12448, 12469, 12490, 12511, 12533, 12554, 12576, 12597, 12619, 12640, 12662, 12684, 12706, 12727, 12750, 12771, 12793, 12815, 12837, 12859, 12882, 12904, 12926, 12948, 12971, 12993, 13015, 13038, 13060, 13083, 13105, 13128, 13150, 13173, 13196, 13219, 13241, 13264, 13287, 13310, 13333, 13356, 13379, 13403, 13426, 13449, 13473, 13496, 13519, 13543, 13566, 13590, 13614, 13637, 13661, 13685, 13708, 13732, 13756, 13780, 13804, 13828, 13853, 13877, 13901, 13925, 13949, 13974, 13998, 14022, 14047, 14071, 14096, 14121, 14145, 14170, 14195, 14220, 14244, 14269, 14294, 14319, 14345, 14370, 14395, 14420, 14446, 14471, 14496, 14522, 14547, 14573, 14598, 14624, 14650, 14676, 14701, 14727, 14753, 14779, 14805, 14831, 14857, 14883, 14910, 14936, 14962, 14988, 15015, 15041, 15068, 15094, 15121, 15147, 15174, 15201, 15228, 15255, 15282, 15308, 15336, 15363, 15390, 15417, 15445, 15472, 15499, 15527, 15555, 15582, 15610, 15638, 15666, 15693, 15721, 15750, 15778, 15806, 15834, 15862, 15891, 15919, 15948, 15976, 16005, 16034, 16062, 16091, 16120, 16149, 16178, 16207, 16236, 16265, 16294, 16324, 16353, 16383, 16412, 16442, 16472, 16502, 16531, 16561, 16591, 16622, 16652, 16682, 16712, 16743, 16773, 16804, 16834, 16865, 16896, 16926, 16957, 16988, 17019, 17050, 17081, 17113, 17144, 17175, 17207, 17238, 17270, 17301, 17333, 17365, 17397, 17429, 17461, 17493, 17525, 17558, 17590, 17623, 17655, 17688, 17721, 17754, 17786, 17820, 17853, 17886, 17919, 17952, 17985, 18018, 18052, 18085, 18119, 18152, 18186, 18220, 18253, 18287, 18321, 18355, 18389, 18423, 18457, 18491, 18526, 18560, 18595, 18629, 18664, 18698, 18733, 18768, 18803, 18838, 18873, 18909, 18944, 18979, 19015, 19050, 19086, 19121, 19157, 19193, 19229, 19265, 19301, 19337, 19374, 19410, 19447, 19483, 19520, 19557, 19593, 19630, 19667, 19704, 19741, 19778, 19815, 19853, 19890, 19927, 19964, 20001, 20039, 20076, 20113, 20150, 20188, 20225, 20263, 20301, 20339, 20377, 20415, 20454, 20493, 20532, 20571, 20610, 20649, 20689, 20728, 20768, 20807, 20847, 20887, 20926, 20966, 21006, 21046, 21086, 21127, 21167, 21208, 21248, 21289, 21330, 21370, 21411, 21453, 21494, 21536, 21577, 21619, 21661, 21704, 21746, 21788, 21831, 21873, 21916, 21958, 22001, 22044, 22087, 22131, 22174, 22218, 22262, 22305, 22349, 22393, 22437, 22481, 22526, 22571, 22616, 22661, 22706, 22751, 22796, 22842, 22888, 22934, 22980, 23026, 23072, 23118, 23165, 23211, 23258, 23305, 23352, 23399, 23447, 23495, 23542, 23590, 23638, 23687, 23735, 23784, 23832, 23881, 23930, 23979, 24028, 24077, 24127, 24176, 24226, 24276, 24326, 24376, 24427, 24477, 24528, 24579, 24630, 24681, 24732, 24784, 24836, 24888, 24940, 24992, 25044, 25096, 25149, 25202, 25254, 25308, 25361, 25414, 25468, 25522, 25576, 25630, 25685, 25739, 25794, 25848, 25903, 25958, 26014, 26069, 26125, 26180, 26236, 26292, 26349, 26405, 26462, 26519, 26576, 26633, 26690, 26748, 26806, 26864, 26922, 26980, 27038, 27097, 27156, 27215, 27275, 27334, 27394, 27454, 27514, 27574, 27635, 27695, 27756, 27817, 27879, 27940, 28002, 28064, 28126, 28188, 28251, 28314, 28377, 28440, 28503, 28567, 28631, 28695, 28759, 28823, 28887, 28952, 29017, 29082, 29147, 29213, 29279, 29344, 29410, 29476, 29543, 29610, 29677, 29745, 29812, 29879, 29947, 30015, 30082, 30150, 30218, 30287, 30356, 30425, 30494, 30564, 30633, 30703, 30774, 30844, 30916, 30987, 31058, 31130, 31202, 31275, 31347, 31420, 31493, 31567, 31641, 31715, 31789, 31864, 31939, 32014, 32089, 32165, 32241, 32317, 32394, 32470, 32547, 32625, 32703, 32781, 32859, 32937, 33016, 33095, 33175, 33254, 33334, 33415, 33495, 33576, 33657, 33739, 33822, 33904, 33986, 34069, 34152, 34237, 34320, 34404, 34488, 34573, 34658, 34743, 34828, 34914, 35000, 35087, 35174, 35261, 35349, 35437, 35525, 35614, 35703, 35792, 35882, 35973, 36063, 36154, 36245, 36337, 36429, 36522, 36614, 36708, 36802, 36896, 36991, 37086, 37182, 37278, 37374, 37471, 37568, 37665, 37763, 37861, 37959, 38058, 38157, 38257, 38357, 38456, 38557, 38658, 38759, 38860, 38962, 39064, 39166, 39269, 39372, 39475, 39577, 39680, 39784, 39887, 39990, 40092, 40194, 40297, 40400, 40505, 40609, 40716, 40824, 40933, 41042, 41153, 41264, 41377, 41490, 41603, 41717, 41832, 41947, 42063, 42180, 42296, 42414, 42533, 42652, 42772, 42893, 43014, 43136, 43258, 43382, 43506, 43630, 43756, 43881, 44007, 44133, 44261, 44389, 44517, 44647, 44777, 44909, 45040, 45171, 45304, 45437, 45572, 45707, 45843, 45979, 46115, 46253, 46391, 46529, 46669, 46810, 46951, 47093, 47236, 47379, 47523, 47669, 47814, 47960, 48107, 48255, 48404, 48553, 48704, 48855, 49007, 49160, 49313, 49467, 49622, 49777, 49934, 50090, 50247, 50405, 50564, 50725, 50886, 51048, 51211, 51375, 51541, 51708, 51877, 52046, 52216, 52387, 52559, 52732, 52906, 53081, 53257, 53434, 53612, 53792, 53972, 54154, 54335, 54519, 54703, 54889, 55073, 55261, 55448, 55636, 55824, 56015, 56207, 56400, 56596, 56794, 56993, 57193, 57395, 57598, 57801, 58007, 58213, 58420, 58629, 58839, 59052, 59265, 59479, 59693, 59908, 60123, 60338, 60555, 60774, 60996, 61220, 61446, 61675, 61905, 62137, 62372, 62608, 62847, 63086, 63325, 63568, 63812, 64058, 64305, 64556, 64808, 65061, 65317, 65575, 65834, 66094, 66357, 66623, 66888, 67157, 67426, 67699, 67974, 68251, 68530, 68813, 69096, 69380, 69667, 69955, 70246, 70539, 70835, 71132, 71433, 71736, 72041, 72350, 72661, 72973, 73289, 73609, 73930, 74255, 74580, 74907, 75238, 75571, 75906, 76243, 76584, 76927, 77275, 77623, 77975, 78328, 78683, 79037, 79391, 79742, 80088, 80428, 80765, 81113, 81472, 81841, 82219, 82605, 82998, 83397, 83800, 84208, 84619, 85035, 85460, 85884, 86314, 86751, 87190, 87638, 88088, 88542, 89002, 89466, 89936, 90408, 90884, 91365, 91852, 92342, 92836, 93334, 93835, 94341, 94851, 95364, 95885, 96406, 96933, 97460, 97993, 98526, 99060, 99591, 100113, 100617, 101135, 101663, 102213, 102794, 103397, 104015, 104645, 105288, 105938, 106601, 107271, 107951, 108640, 109338, 110044, 110757, 111483, 112220, 112967, 113725, 114496, 115275, 116061, 116857, 117667, 118493, 119313, 120134, 120960, 121800, 122662, 123551, 124454, 125370, 126298, 127249, 128207, 129183, 130179, 131192, 132216, 133260, 134320, 135398, 136497, 137611, 138744, 139890, 141058, 142238, 143447, 144679, 145933, 147199, 148497, 149816, 151152, 152510, 153896, 155300, 156717, 158135, 159534, 160774, 161876, 163032, 164406, 165956, 167576, 169250, 170968, 172735, 174548, 176390, 178300, 180241, 182224, 184256, 186334, 188469, 190664, 192917, 195211, 197556, 199927, 202287, 204730, 207292, 209959, 212702, 215558, 218474, 221471, 224555, 227734, 231016, 234383, 237844, 241263, 244684, 248447, 252372, 256448, 260685, 265077, 269636, 274371, 279267, 284304, 289598, 295130, 300859, 306794, 312993, 319282, 324291, 330680, 337987, 345761, 354075, 362735, 371930, 381774, 392248, 403003, 414482, 427083, 440610, 455146, 470919, 486197, 503788, 523998, 546352, 570909, 598830, 630037, 662580, 703809, 751763, 806985, 875277, 962703, 1075532, 1238933, 1497571, 2043071, Integer.MAX_VALUE)

  private val DENSITY_SIZE: Long = math.pow(2, 24).toLong

  def computeHash(roadSegments: RDD[RoadSegment], angleBytes: Int, geoHashBytes: Int, sc: SparkContext): RDD[(Seq[Boolean], Seq[RoadSegment])] = {
    val defaultLength = 5
    roadSegments.flatMap(roadSegment => {
      // 0 or 1 node roads are considered faulty
      if (roadSegment.coordinates.length > 1) {
        // Compute length using the haversine function, sliding window on the sorted nodes
        val length = roadSegment.coordinates.sliding(2).collect {
          case Seq(coordinates1, coordinates2) => haversine(coordinates1, coordinates2)
        }.sum
        val elevation = computeElevation(roadSegment.tags)
        val midpointShift = elevation.map(exists => computeMidpointShift(exists))
        val middleNode = if (roadSegment.coordinates.length > 2) roadSegment.coordinates(roadSegment.coordinates.length / 2) else calculateMidPoint(roadSegment.coordinates)
        val midpoint = {
          val originalMidpoint = calculateMidPoint(roadSegment.coordinates)
          if (midpointShift.nonEmpty) {
            shiftMidpoint(roadSegment.coordinates, originalMidpoint, geoHashDiagonalLength(geoHashBytes - 4) * midpointShift.get._1)
          }
          else originalMidpoint
        }

        val roadAngle = angle(roadSegment.coordinates.head, roadSegment.coordinates.last)

        if (roadSegment.coordinates.head == roadSegment.coordinates.last || haversine(roadSegment.coordinates.head, roadSegment.coordinates.last) < haversine(roadSegment.coordinates.head, middleNode)) {
          // For circular roads and V-shaped roads angle is not needed
          // Instead an optimisation is added where these bytes are used for length instead
          val distributedLength = findIntervalIndex(0, LENGTH_INTERVALS_2048.length - 1, length, LENGTH_INTERVALS_2048)
          val lengthHash = computeBinaryHash(distributedLength, defaultLength + angleBytes)
          val geoHash = GeoHashHilbert.encode(midpoint.longitude.toDouble, midpoint.latitude.toDouble, geoHashBytes, midpointShift)
          val compositeHash = geoHash ++ lengthHash
          Some(compositeHash -> Seq(roadSegment))
        }
        else {
          val angleHash = binarySearch(roadAngle, (0, 2 * math.Pi), angleBytes)
          val distributedLength = findIntervalIndex(0, LENGTH_INTERVALS_32.length - 1, length, LENGTH_INTERVALS_32)
          val lengthHash = computeBinaryHash(distributedLength, defaultLength)

          val geoHash = GeoHashHilbert.encode(midpoint.longitude.toDouble, midpoint.latitude.toDouble, geoHashBytes, midpointShift)
          val compositeHash = geoHash ++ angleHash ++ lengthHash
          Some(compositeHash -> Seq(roadSegment))
        }
      }
      else None
    }).reduceByKey((a, b) => a ++ b)
  }


  // Code used for LSH test using minHash, unused in core algorithm
  def minHashTest(roads: RDD[RoadSegment], precision: Int, k: Int): RDD[(Seq[Boolean], Seq[RoadSegment])] = {
    val xxHashFactory: XXHashFactory = XXHashFactory.safeInstance()
    val xxHasher: Broadcast[XXHashFactory] = roads.context.broadcast(xxHashFactory)


    roads.flatMap { road =>
      if (road.coordinates.length > 1) {
        val geohashes = geohashPolyline(road.coordinates, precision, 8)

        val minHashValues = Array.fill(k)(Long.MaxValue)

        val broadcastedXXHashFactory = xxHasher.value

        for (geohash <- geohashes) {
          for (i <- 0 until k) {
            val geohashBytes = geohash.map(if (_) 1.toByte else 0.toByte).toArray
            val hashValue = broadcastedXXHashFactory.hash64().hash(geohashBytes, 0, geohashBytes.length, i.toLong)
            minHashValues(i) = math.min(minHashValues(i), hashValue)
          }
        }
        val binaryMinHashValues = minHashValues.map(value => !(value % 2 == 0))

        Some(binaryMinHashValues.toSeq -> Seq(road))
      }
      else None
    }.reduceByKey((a, b) => a ++ b)
  }

  def geoHashTileToIndex(roads: RDD[RoadSegment]): RDD[(Seq[Boolean], Seq[Boolean])] = {
    // Count number of roads within each 28 precision Geohash tile (hilbert)
    val roadsInHash = roads
      .map { road =>
        val midpoint = if (road.coordinates.length > 2) road.coordinates(road.coordinates.length / 2) else calculateMidPoint(road.coordinates)
        GeoHashHilbert.encode(midpoint.longitude.toDouble, midpoint.latitude.toDouble, 28, None) -> 1
      }
      .reduceByKey(_ + _)

    // Count number of roads in larger 24 precision Geohash tile (hilbert)
    val roadsInLargerHash = roadsInHash.map { case (key, value) => (key.take(24), value) }.reduceByKey(_ + _)

    // Group these together to get access to both counts in one rdd
    val hashRDDKeyed = roadsInHash.map { case (key, value) => (key.take(24), (key, value)) }
    val groupedRDD = hashRDDKeyed.cogroup(roadsInLargerHash)
    val joinedRDD = groupedRDD.flatMap { case (_, (rdd1Values, rdd2Values)) =>
      for {
        (key, value1) <- rdd1Values
        value2 <- rdd2Values
      } yield (key, value1, value2)
    }

    // Filter into dense/ sparse based on predicates, currently set to 10 for small tile and 320 for large tile
    val denseRoadsRDD = joinedRDD.filter {
      case (_, roads, parentRoads) =>
        roads > 10 || parentRoads > 320
    }

    val denseCount = denseRoadsRDD.count()
    logger.info(s"Dense count $denseCount")

    val sparseRoadsRDD = joinedRDD.subtract(denseRoadsRDD)
    logger.info(s"Sparse count ${sparseRoadsRDD.count()}")

    // Create the new hashes based zipWithIndex (future work to improve this index assignment), 28 -> 24 bits
    val denseRoadMappingsRDD = denseRoadsRDD.map {
      case (hash, _, _) => hash
    }.zipWithIndex().map {
      case (hash, newHashIndex) =>
        val newHash = computeBinaryHash(newHashIndex, 24)
        (hash, newHash)
    }

    val sparseRoadMappingsRDD = sparseRoadsRDD.map {
      case (hash, _, _) => hash
    }.zipWithIndex().map {
      case (hash, index) =>
        val newHashIndex = index % (DENSITY_SIZE - denseCount) + denseCount
        val newHash = computeBinaryHash(newHashIndex, 24)
        (hash, newHash)
    }
    val denseSparseToIndexRDD = denseRoadMappingsRDD ++ sparseRoadMappingsRDD

    // Optional code to collect info on size of mapping in case it is to be stored as artifact
//    val baos = new ByteArrayOutputStream()
//    val oos = new ObjectOutputStream(baos)
//    oos.writeObject(denseSparseToIndexRDD.collect().toMap)
//    oos.close();
//    val artifactSize = baos.size()

    denseSparseToIndexRDD
  }

  // Helper method to convert a number into it's binary equivalent, represented as Seq[Boolean]
  def computeBinaryHash(number: BigInt, binaryLength: Int): Seq[Boolean] = {
    val modulo = BigInt(1) << binaryLength
    val binaryString = (number % modulo).toString(2)
    val padding = binaryLength - binaryString.length
    val paddedBinaryString = "0" * padding + binaryString
    paddedBinaryString.map(_ == '1')
  }

  // Method to calculate the road length distributions, results are put in LENGTH_INTERVALS_32 and LIMITS2048
  // Head of result is replaced with 0, Integer.MAX_VALUE is appended as last value
  private def optimizeLength(roads: RDD[RoadSegment], binaryLength: Int): Seq[Int] = {
    val lengths = roads.flatMap { road =>
      if (road.coordinates.length > 1) {
        Some(road.coordinates.sliding(2).collect {
          case Seq(coordinates1, coordinates2) => haversine(coordinates1, coordinates2)
        }.sum)
      } else None
    }.collect().sorted.toSeq
    val chunkSize = lengths.length / binaryLength
    val remainder = lengths.length % binaryLength

    val subSeqs: Seq[Seq[Double]] = (0 until binaryLength).map { i =>
      val start = i * chunkSize + math.min(i, remainder)
      val end = (i + 1) * chunkSize + math.min(i + 1, remainder)
      lengths.slice(start, end)
    }
    subSeqs.map(_.head.ceil).map(_.toInt)
  }

  // Helper function to compute elevation based on the tags of the feature
  private def computeElevation(tags: Map[String, String]): Option[Float] = {
    try {
      (ELEVATION_TAGS.flatMap(tag => tags.get(tag)).headOption match {
        case Some(elev) if elev.contains(";") => Some(elev.split(";").head)
        case Some(elev) => Some(elev)
        case None => None
      }).map(_.toFloat)
    } catch {
      case _: Exception => None
    }
  }

  // Manually computed shiftMap for elevation to index within geohash tile
  private val shiftMap: Map[BigDecimal, (Int, Int)] = Map(
    BigDecimal(-2.1) -> ((-2, 0)),
    BigDecimal(-2.2) -> ((-2, 1)),
    BigDecimal(-2.3) -> ((-2, 2)),
    BigDecimal(-2.4) -> ((-2, 3)),
    BigDecimal(-2.5) -> ((-2, 4)),
    BigDecimal(-2.6) -> ((-2, 5)),
    BigDecimal(-2.7) -> ((-2, 6)),
    BigDecimal(-2.8) -> ((-2, 7)),
    BigDecimal(-2.9) -> ((-2, 8)),
    BigDecimal(-1.1) -> ((-2, 9)),
    BigDecimal(-1.2) -> ((-2, 10)),
    BigDecimal(-1.3) -> ((-2, 11)),
    BigDecimal(-1.4) -> ((-2, 12)),
    BigDecimal(-1.5) -> ((-2, 13)),
    BigDecimal(-1.6) -> ((-2, 14)),
    BigDecimal(-1.7) -> ((-2, 15)),
    BigDecimal(-1.8) -> ((-1, 0)),
    BigDecimal(-1.9) -> ((-1, 1)),
    BigDecimal(-0.1) -> ((-1, 2)),
    BigDecimal(-0.2) -> ((-1, 3)),
    BigDecimal(-0.3) -> ((-1, 4)),
    BigDecimal(-0.4) -> ((-1, 5)),
    BigDecimal(-0.5) -> ((-1, 6)),
    BigDecimal(-0.6) -> ((-1, 7)),
    BigDecimal(-0.7) -> ((-1, 8)),
    BigDecimal(-0.8) -> ((-1, 9)),
    BigDecimal(-0.9) -> ((-1, 10)),
    BigDecimal(0.1) -> ((-1, 11)),
    BigDecimal(0.2) -> ((-1, 12)),
    BigDecimal(0.3) -> ((-1, 13)),
    BigDecimal(0.4) -> ((-1, 14)),
    BigDecimal(0.5) -> ((-1, 15)),
    BigDecimal(0.6) -> ((1, 0)),
    BigDecimal(0.7) -> ((1, 1)),
    BigDecimal(0.8) -> ((1, 2)),
    BigDecimal(0.9) -> ((1, 3)),
    BigDecimal(1.1) -> ((1, 4)),
    BigDecimal(1.2) -> ((1, 5)),
    BigDecimal(1.3) -> ((1, 6)),
    BigDecimal(1.4) -> ((1, 7)),
    BigDecimal(1.5) -> ((1, 8)),
    BigDecimal(1.6) -> ((1, 9)),
    BigDecimal(1.7) -> ((1, 10)),
    BigDecimal(1.8) -> ((1, 11)),
    BigDecimal(1.9) -> ((1, 12)),
    BigDecimal(2.1) -> ((1, 13)),
    BigDecimal(2.2) -> ((1, 14)),
    BigDecimal(2.3) -> ((1, 15)),
    BigDecimal(2.4) -> ((2, 0)),
    BigDecimal(2.5) -> ((2, 1)),
    BigDecimal(2.6) -> ((2, 2)),
    BigDecimal(2.7) -> ((2, 3)),
    BigDecimal(2.8) -> ((2, 4)),
    BigDecimal(2.9) -> ((2, 5)),
    BigDecimal(3.5) -> ((2, 6)),
    BigDecimal(4.5) -> ((2, 7)),
    BigDecimal(5.5) -> ((2, 8)),
    BigDecimal(6.5) -> ((2, 9)),
    BigDecimal(7.5) -> ((2, 10)),
    BigDecimal(8.5) -> ((2, 11)),
    BigDecimal(9.5) -> ((2, 12))
  )

  // Compute the midpoint shift based on the elevation. Whole numbers are not shifted, but 5 is added to handle the whole negative values
  // For decimal values the midpoint shift is obtained  instead
  private def computeMidpointShift(elevation: BigDecimal): (Int, Int) = {
    if (elevation.isWhole()) (0, math.max(elevation.toInt + 5, 0))
    else {
      val roundedElevation = elevation.setScale(1, RoundingMode.HALF_DOWN)
      shiftMap.getOrElse(roundedElevation, (2, 13))
    }
  }


  // Recursive helper function to find the interval index within the length limit vector
  @tailrec
  private def findIntervalIndex(left: Int, right: Int, target: Double, limits: Seq[Int]): Int = {
    if (left == right) {
      left
    } else {
      val mid = left + (right - left) / 2
      if (limits(mid) <= target && limits(mid + 1) > target) {
        mid
      } else if (limits(mid) > target) {
        findIntervalIndex(left, mid, target, limits)
      } else {
        findIntervalIndex(mid + 1, right, target, limits)
      }
    }
  }
}
