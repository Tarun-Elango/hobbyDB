// Lean compiler output
// Module: KvStore.Map
// Imports: public import Init public meta import Init
#include <lean/lean.h>
#if defined(__clang__)
#pragma clang diagnostic ignored "-Wunused-parameter"
#pragma clang diagnostic ignored "-Wunused-label"
#elif defined(__GNUC__) && !defined(__CLANG__)
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wunused-label"
#pragma GCC diagnostic ignored "-Wunused-but-set-variable"
#endif
#ifdef __cplusplus
extern "C" {
#endif
lean_object* l_List_countP_go___redArg(lean_object*, lean_object*, lean_object*);
LEAN_EXPORT lean_object* lp_kvstore_KvStore_getData___redArg(lean_object*, lean_object*);
LEAN_EXPORT lean_object* lp_kvstore_KvStore_getData(lean_object*, lean_object*, lean_object*, lean_object*);
LEAN_EXPORT lean_object* lp_kvstore_KvStore_setData___redArg(lean_object*, lean_object*, lean_object*, lean_object*, lean_object*);
LEAN_EXPORT lean_object* lp_kvstore_KvStore_setData(lean_object*, lean_object*, lean_object*, lean_object*, lean_object*, lean_object*, lean_object*);
LEAN_EXPORT lean_object* lp_kvstore_KvStore_deleteData___redArg(lean_object*, lean_object*, lean_object*, lean_object*);
LEAN_EXPORT lean_object* lp_kvstore_KvStore_deleteData(lean_object*, lean_object*, lean_object*, lean_object*, lean_object*, lean_object*);
LEAN_EXPORT lean_object* lp_kvstore_KvStore_emptyData___redArg();
LEAN_EXPORT lean_object* lp_kvstore_KvStore_emptyData___redArg___boxed(lean_object*);
LEAN_EXPORT lean_object* lp_kvstore_KvStore_emptyData(lean_object*, lean_object*, lean_object*);
LEAN_EXPORT lean_object* lp_kvstore_KvStore_emptyData___boxed(lean_object*, lean_object*, lean_object*);
LEAN_EXPORT lean_object* lp_kvstore_KvStore_ofList___redArg(lean_object*, lean_object*, lean_object*);
LEAN_EXPORT lean_object* lp_kvstore_KvStore_ofList(lean_object*, lean_object*, lean_object*, lean_object*, lean_object*);
LEAN_EXPORT lean_object* lp_kvstore_KvStore_instInhabitedStore_default___redArg___lam__0(lean_object*);
LEAN_EXPORT lean_object* lp_kvstore_KvStore_instInhabitedStore_default___redArg___lam__0___boxed(lean_object*);
static const lean_closure_object lp_kvstore_KvStore_instInhabitedStore_default___redArg___closed__0_value = {.m_header = {.m_rc = 0, .m_cs_sz = sizeof(lean_closure_object) + sizeof(void*)*0, .m_other = 0, .m_tag = 245}, .m_fun = (void*)lp_kvstore_KvStore_instInhabitedStore_default___redArg___lam__0___boxed, .m_arity = 1, .m_num_fixed = 0, .m_objs = {} };
static const lean_object* lp_kvstore_KvStore_instInhabitedStore_default___redArg___closed__0 = (const lean_object*)&lp_kvstore_KvStore_instInhabitedStore_default___redArg___closed__0_value;
LEAN_EXPORT lean_object* lp_kvstore_KvStore_instInhabitedStore_default___redArg();
LEAN_EXPORT lean_object* lp_kvstore_KvStore_instInhabitedStore_default___redArg___boxed(lean_object*);
LEAN_EXPORT lean_object* lp_kvstore_KvStore_instInhabitedStore_default(lean_object*, lean_object*, lean_object*);
LEAN_EXPORT lean_object* lp_kvstore_KvStore_instInhabitedStore_default___boxed(lean_object*, lean_object*, lean_object*);
LEAN_EXPORT lean_object* lp_kvstore_KvStore_instInhabitedStore___redArg();
LEAN_EXPORT lean_object* lp_kvstore_KvStore_instInhabitedStore___redArg___boxed(lean_object*);
LEAN_EXPORT lean_object* lp_kvstore_KvStore_instInhabitedStore(lean_object*, lean_object*, lean_object*);
LEAN_EXPORT lean_object* lp_kvstore_KvStore_instInhabitedStore___boxed(lean_object*, lean_object*, lean_object*);
static const lean_closure_object lp_kvstore_KvStore_New___redArg___closed__0_value = {.m_header = {.m_rc = 0, .m_cs_sz = sizeof(lean_closure_object) + sizeof(void*)*2, .m_other = 0, .m_tag = 245}, .m_fun = (void*)lp_kvstore_KvStore_emptyData___boxed, .m_arity = 3, .m_num_fixed = 2, .m_objs = {((lean_object*)(((size_t)(0) << 1) | 1)),((lean_object*)(((size_t)(0) << 1) | 1))} };
static const lean_object* lp_kvstore_KvStore_New___redArg___closed__0 = (const lean_object*)&lp_kvstore_KvStore_New___redArg___closed__0_value;
LEAN_EXPORT lean_object* lp_kvstore_KvStore_New___redArg();
LEAN_EXPORT lean_object* lp_kvstore_KvStore_New___redArg___boxed(lean_object*);
LEAN_EXPORT lean_object* lp_kvstore_KvStore_New(lean_object*, lean_object*, lean_object*);
LEAN_EXPORT lean_object* lp_kvstore_KvStore_New___boxed(lean_object*, lean_object*, lean_object*);
LEAN_EXPORT lean_object* lp_kvstore_KvStore_Get___redArg(lean_object*, lean_object*);
LEAN_EXPORT lean_object* lp_kvstore_KvStore_Get(lean_object*, lean_object*, lean_object*, lean_object*, lean_object*);
LEAN_EXPORT lean_object* lp_kvstore_KvStore_Get___boxed(lean_object*, lean_object*, lean_object*, lean_object*, lean_object*);
LEAN_EXPORT lean_object* lp_kvstore_KvStore_Set___redArg(lean_object*, lean_object*, lean_object*, lean_object*);
LEAN_EXPORT lean_object* lp_kvstore_KvStore_Set(lean_object*, lean_object*, lean_object*, lean_object*, lean_object*, lean_object*);
LEAN_EXPORT lean_object* lp_kvstore_KvStore_Delete___redArg(lean_object*, lean_object*, lean_object*);
LEAN_EXPORT lean_object* lp_kvstore_KvStore_Delete(lean_object*, lean_object*, lean_object*, lean_object*, lean_object*);
LEAN_EXPORT lean_object* lp_kvstore_KvStore_Replace___redArg(lean_object*, lean_object*);
LEAN_EXPORT lean_object* lp_kvstore_KvStore_Replace(lean_object*, lean_object*, lean_object*, lean_object*, lean_object*);
LEAN_EXPORT lean_object* lp_kvstore_KvStore_Replace___boxed(lean_object*, lean_object*, lean_object*, lean_object*, lean_object*);
LEAN_EXPORT uint8_t lp_kvstore_KvStore_Len___redArg___lam__0(lean_object*, lean_object*);
LEAN_EXPORT lean_object* lp_kvstore_KvStore_Len___redArg___lam__0___boxed(lean_object*, lean_object*);
LEAN_EXPORT lean_object* lp_kvstore_KvStore_Len___redArg(lean_object*, lean_object*);
LEAN_EXPORT lean_object* lp_kvstore_KvStore_Len(lean_object*, lean_object*, lean_object*, lean_object*, lean_object*);
LEAN_EXPORT lean_object* lp_kvstore_KvStore_Len___boxed(lean_object*, lean_object*, lean_object*, lean_object*, lean_object*);
LEAN_EXPORT lean_object* lp_kvstore_KvStore_getData___redArg(lean_object* v_data_1_, lean_object* v_k_2_){
_start:
{
lean_object* v___x_3_; 
v___x_3_ = lean_apply_1(v_data_1_, v_k_2_);
return v___x_3_;
}
}
LEAN_EXPORT lean_object* lp_kvstore_KvStore_getData(lean_object* v_00_u03b1_4_, lean_object* v_00_u03b2_5_, lean_object* v_data_6_, lean_object* v_k_7_){
_start:
{
lean_object* v___x_8_; 
v___x_8_ = lean_apply_1(v_data_6_, v_k_7_);
return v___x_8_;
}
}
LEAN_EXPORT lean_object* lp_kvstore_KvStore_setData___redArg(lean_object* v_inst_9_, lean_object* v_data_10_, lean_object* v_k_11_, lean_object* v_v_12_, lean_object* v_k_x27_13_){
_start:
{
lean_object* v___x_14_; uint8_t v___x_15_; 
lean_inc(v_k_x27_13_);
v___x_14_ = lean_apply_2(v_inst_9_, v_k_x27_13_, v_k_11_);
v___x_15_ = lean_unbox(v___x_14_);
if (v___x_15_ == 0)
{
lean_object* v___x_16_; 
lean_dec(v_v_12_);
v___x_16_ = lean_apply_1(v_data_10_, v_k_x27_13_);
return v___x_16_;
}
else
{
lean_object* v___x_17_; 
lean_dec(v_k_x27_13_);
lean_dec_ref(v_data_10_);
v___x_17_ = lean_alloc_ctor(1, 1, 0);
lean_ctor_set(v___x_17_, 0, v_v_12_);
return v___x_17_;
}
}
}
LEAN_EXPORT lean_object* lp_kvstore_KvStore_setData(lean_object* v_00_u03b1_18_, lean_object* v_00_u03b2_19_, lean_object* v_inst_20_, lean_object* v_data_21_, lean_object* v_k_22_, lean_object* v_v_23_, lean_object* v_k_x27_24_){
_start:
{
lean_object* v___x_25_; 
v___x_25_ = lp_kvstore_KvStore_setData___redArg(v_inst_20_, v_data_21_, v_k_22_, v_v_23_, v_k_x27_24_);
return v___x_25_;
}
}
LEAN_EXPORT lean_object* lp_kvstore_KvStore_deleteData___redArg(lean_object* v_inst_26_, lean_object* v_data_27_, lean_object* v_k_28_, lean_object* v_k_x27_29_){
_start:
{
lean_object* v___x_30_; uint8_t v___x_31_; 
lean_inc(v_k_x27_29_);
v___x_30_ = lean_apply_2(v_inst_26_, v_k_x27_29_, v_k_28_);
v___x_31_ = lean_unbox(v___x_30_);
if (v___x_31_ == 0)
{
lean_object* v___x_32_; 
v___x_32_ = lean_apply_1(v_data_27_, v_k_x27_29_);
return v___x_32_;
}
else
{
lean_object* v___x_33_; 
lean_dec(v_k_x27_29_);
lean_dec_ref(v_data_27_);
v___x_33_ = lean_box(0);
return v___x_33_;
}
}
}
LEAN_EXPORT lean_object* lp_kvstore_KvStore_deleteData(lean_object* v_00_u03b1_34_, lean_object* v_00_u03b2_35_, lean_object* v_inst_36_, lean_object* v_data_37_, lean_object* v_k_38_, lean_object* v_k_x27_39_){
_start:
{
lean_object* v___x_40_; 
v___x_40_ = lp_kvstore_KvStore_deleteData___redArg(v_inst_36_, v_data_37_, v_k_38_, v_k_x27_39_);
return v___x_40_;
}
}
LEAN_EXPORT lean_object* lp_kvstore_KvStore_emptyData___redArg(){
_start:
{
lean_object* v___x_42_; 
v___x_42_ = lean_box(0);
return v___x_42_;
}
}
LEAN_EXPORT lean_object* lp_kvstore_KvStore_emptyData___redArg___boxed(lean_object* v___dummy_43_){
_start:
{
lean_object* v_res_44_; 
v_res_44_ = lp_kvstore_KvStore_emptyData___redArg();
return v_res_44_;
}
}
LEAN_EXPORT lean_object* lp_kvstore_KvStore_emptyData(lean_object* v_00_u03b1_45_, lean_object* v_00_u03b2_46_, lean_object* v_x_47_){
_start:
{
lean_object* v___x_48_; 
v___x_48_ = lean_box(0);
return v___x_48_;
}
}
LEAN_EXPORT lean_object* lp_kvstore_KvStore_emptyData___boxed(lean_object* v_00_u03b1_49_, lean_object* v_00_u03b2_50_, lean_object* v_x_51_){
_start:
{
lean_object* v_res_52_; 
v_res_52_ = lp_kvstore_KvStore_emptyData(v_00_u03b1_49_, v_00_u03b2_50_, v_x_51_);
lean_dec(v_x_51_);
return v_res_52_;
}
}
LEAN_EXPORT lean_object* lp_kvstore_KvStore_ofList___redArg(lean_object* v_inst_53_, lean_object* v_x_54_, lean_object* v_a_55_){
_start:
{
if (lean_obj_tag(v_x_54_) == 0)
{
lean_object* v___x_56_; 
lean_dec(v_a_55_);
lean_dec_ref(v_inst_53_);
v___x_56_ = lean_box(0);
return v___x_56_;
}
else
{
lean_object* v_head_57_; lean_object* v_tail_58_; lean_object* v_fst_59_; lean_object* v_snd_60_; lean_object* v___x_61_; lean_object* v___x_62_; 
v_head_57_ = lean_ctor_get(v_x_54_, 0);
lean_inc(v_head_57_);
v_tail_58_ = lean_ctor_get(v_x_54_, 1);
lean_inc(v_tail_58_);
lean_dec_ref_known(v_x_54_, 2);
v_fst_59_ = lean_ctor_get(v_head_57_, 0);
lean_inc(v_fst_59_);
v_snd_60_ = lean_ctor_get(v_head_57_, 1);
lean_inc(v_snd_60_);
lean_dec(v_head_57_);
lean_inc_ref(v_inst_53_);
v___x_61_ = lean_alloc_closure((void*)(lp_kvstore_KvStore_ofList___redArg), 3, 2);
lean_closure_set(v___x_61_, 0, v_inst_53_);
lean_closure_set(v___x_61_, 1, v_tail_58_);
v___x_62_ = lp_kvstore_KvStore_setData___redArg(v_inst_53_, v___x_61_, v_fst_59_, v_snd_60_, v_a_55_);
return v___x_62_;
}
}
}
LEAN_EXPORT lean_object* lp_kvstore_KvStore_ofList(lean_object* v_00_u03b1_63_, lean_object* v_00_u03b2_64_, lean_object* v_inst_65_, lean_object* v_x_66_, lean_object* v_a_67_){
_start:
{
lean_object* v___x_68_; 
v___x_68_ = lp_kvstore_KvStore_ofList___redArg(v_inst_65_, v_x_66_, v_a_67_);
return v___x_68_;
}
}
LEAN_EXPORT lean_object* lp_kvstore_KvStore_instInhabitedStore_default___redArg___lam__0(lean_object* v_x_69_){
_start:
{
lean_object* v___x_70_; 
v___x_70_ = lean_box(0);
return v___x_70_;
}
}
LEAN_EXPORT lean_object* lp_kvstore_KvStore_instInhabitedStore_default___redArg___lam__0___boxed(lean_object* v_x_71_){
_start:
{
lean_object* v_res_72_; 
v_res_72_ = lp_kvstore_KvStore_instInhabitedStore_default___redArg___lam__0(v_x_71_);
lean_dec(v_x_71_);
return v_res_72_;
}
}
LEAN_EXPORT lean_object* lp_kvstore_KvStore_instInhabitedStore_default___redArg(){
_start:
{
lean_object* v___f_75_; 
v___f_75_ = ((lean_object*)(lp_kvstore_KvStore_instInhabitedStore_default___redArg___closed__0));
return v___f_75_;
}
}
LEAN_EXPORT lean_object* lp_kvstore_KvStore_instInhabitedStore_default___redArg___boxed(lean_object* v___dummy_76_){
_start:
{
lean_object* v_res_77_; 
v_res_77_ = lp_kvstore_KvStore_instInhabitedStore_default___redArg();
return v_res_77_;
}
}
LEAN_EXPORT lean_object* lp_kvstore_KvStore_instInhabitedStore_default(lean_object* v_00_u03b1_78_, lean_object* v_00_u03b2_79_, lean_object* v_inst_80_){
_start:
{
lean_object* v___f_81_; 
v___f_81_ = ((lean_object*)(lp_kvstore_KvStore_instInhabitedStore_default___redArg___closed__0));
return v___f_81_;
}
}
LEAN_EXPORT lean_object* lp_kvstore_KvStore_instInhabitedStore_default___boxed(lean_object* v_00_u03b1_82_, lean_object* v_00_u03b2_83_, lean_object* v_inst_84_){
_start:
{
lean_object* v_res_85_; 
v_res_85_ = lp_kvstore_KvStore_instInhabitedStore_default(v_00_u03b1_82_, v_00_u03b2_83_, v_inst_84_);
lean_dec_ref(v_inst_84_);
return v_res_85_;
}
}
LEAN_EXPORT lean_object* lp_kvstore_KvStore_instInhabitedStore___redArg(){
_start:
{
lean_object* v___f_87_; 
v___f_87_ = ((lean_object*)(lp_kvstore_KvStore_instInhabitedStore_default___redArg___closed__0));
return v___f_87_;
}
}
LEAN_EXPORT lean_object* lp_kvstore_KvStore_instInhabitedStore___redArg___boxed(lean_object* v___dummy_88_){
_start:
{
lean_object* v_res_89_; 
v_res_89_ = lp_kvstore_KvStore_instInhabitedStore___redArg();
return v_res_89_;
}
}
LEAN_EXPORT lean_object* lp_kvstore_KvStore_instInhabitedStore(lean_object* v_a_90_, lean_object* v_a_91_, lean_object* v_a_92_){
_start:
{
lean_object* v___f_93_; 
v___f_93_ = ((lean_object*)(lp_kvstore_KvStore_instInhabitedStore_default___redArg___closed__0));
return v___f_93_;
}
}
LEAN_EXPORT lean_object* lp_kvstore_KvStore_instInhabitedStore___boxed(lean_object* v_a_94_, lean_object* v_a_95_, lean_object* v_a_96_){
_start:
{
lean_object* v_res_97_; 
v_res_97_ = lp_kvstore_KvStore_instInhabitedStore(v_a_94_, v_a_95_, v_a_96_);
lean_dec_ref(v_a_96_);
return v_res_97_;
}
}
LEAN_EXPORT lean_object* lp_kvstore_KvStore_New___redArg(){
_start:
{
lean_object* v___x_100_; 
v___x_100_ = ((lean_object*)(lp_kvstore_KvStore_New___redArg___closed__0));
return v___x_100_;
}
}
LEAN_EXPORT lean_object* lp_kvstore_KvStore_New___redArg___boxed(lean_object* v___dummy_101_){
_start:
{
lean_object* v_res_102_; 
v_res_102_ = lp_kvstore_KvStore_New___redArg();
return v_res_102_;
}
}
LEAN_EXPORT lean_object* lp_kvstore_KvStore_New(lean_object* v_00_u03b1_103_, lean_object* v_00_u03b2_104_, lean_object* v_inst_105_){
_start:
{
lean_object* v___x_106_; 
v___x_106_ = ((lean_object*)(lp_kvstore_KvStore_New___redArg___closed__0));
return v___x_106_;
}
}
LEAN_EXPORT lean_object* lp_kvstore_KvStore_New___boxed(lean_object* v_00_u03b1_107_, lean_object* v_00_u03b2_108_, lean_object* v_inst_109_){
_start:
{
lean_object* v_res_110_; 
v_res_110_ = lp_kvstore_KvStore_New(v_00_u03b1_107_, v_00_u03b2_108_, v_inst_109_);
lean_dec_ref(v_inst_109_);
return v_res_110_;
}
}
LEAN_EXPORT lean_object* lp_kvstore_KvStore_Get___redArg(lean_object* v_s_111_, lean_object* v_k_112_){
_start:
{
lean_object* v___x_113_; 
v___x_113_ = lean_apply_1(v_s_111_, v_k_112_);
return v___x_113_;
}
}
LEAN_EXPORT lean_object* lp_kvstore_KvStore_Get(lean_object* v_00_u03b1_114_, lean_object* v_00_u03b2_115_, lean_object* v_inst_116_, lean_object* v_s_117_, lean_object* v_k_118_){
_start:
{
lean_object* v___x_119_; 
v___x_119_ = lean_apply_1(v_s_117_, v_k_118_);
return v___x_119_;
}
}
LEAN_EXPORT lean_object* lp_kvstore_KvStore_Get___boxed(lean_object* v_00_u03b1_120_, lean_object* v_00_u03b2_121_, lean_object* v_inst_122_, lean_object* v_s_123_, lean_object* v_k_124_){
_start:
{
lean_object* v_res_125_; 
v_res_125_ = lp_kvstore_KvStore_Get(v_00_u03b1_120_, v_00_u03b2_121_, v_inst_122_, v_s_123_, v_k_124_);
lean_dec_ref(v_inst_122_);
return v_res_125_;
}
}
LEAN_EXPORT lean_object* lp_kvstore_KvStore_Set___redArg(lean_object* v_inst_126_, lean_object* v_s_127_, lean_object* v_k_128_, lean_object* v_v_129_){
_start:
{
lean_object* v___x_130_; 
v___x_130_ = lean_alloc_closure((void*)(lp_kvstore_KvStore_setData), 7, 6);
lean_closure_set(v___x_130_, 0, lean_box(0));
lean_closure_set(v___x_130_, 1, lean_box(0));
lean_closure_set(v___x_130_, 2, v_inst_126_);
lean_closure_set(v___x_130_, 3, v_s_127_);
lean_closure_set(v___x_130_, 4, v_k_128_);
lean_closure_set(v___x_130_, 5, v_v_129_);
return v___x_130_;
}
}
LEAN_EXPORT lean_object* lp_kvstore_KvStore_Set(lean_object* v_00_u03b1_131_, lean_object* v_00_u03b2_132_, lean_object* v_inst_133_, lean_object* v_s_134_, lean_object* v_k_135_, lean_object* v_v_136_){
_start:
{
lean_object* v___x_137_; 
v___x_137_ = lean_alloc_closure((void*)(lp_kvstore_KvStore_setData), 7, 6);
lean_closure_set(v___x_137_, 0, lean_box(0));
lean_closure_set(v___x_137_, 1, lean_box(0));
lean_closure_set(v___x_137_, 2, v_inst_133_);
lean_closure_set(v___x_137_, 3, v_s_134_);
lean_closure_set(v___x_137_, 4, v_k_135_);
lean_closure_set(v___x_137_, 5, v_v_136_);
return v___x_137_;
}
}
LEAN_EXPORT lean_object* lp_kvstore_KvStore_Delete___redArg(lean_object* v_inst_138_, lean_object* v_s_139_, lean_object* v_k_140_){
_start:
{
lean_object* v___x_141_; 
v___x_141_ = lean_alloc_closure((void*)(lp_kvstore_KvStore_deleteData), 6, 5);
lean_closure_set(v___x_141_, 0, lean_box(0));
lean_closure_set(v___x_141_, 1, lean_box(0));
lean_closure_set(v___x_141_, 2, v_inst_138_);
lean_closure_set(v___x_141_, 3, v_s_139_);
lean_closure_set(v___x_141_, 4, v_k_140_);
return v___x_141_;
}
}
LEAN_EXPORT lean_object* lp_kvstore_KvStore_Delete(lean_object* v_00_u03b1_142_, lean_object* v_00_u03b2_143_, lean_object* v_inst_144_, lean_object* v_s_145_, lean_object* v_k_146_){
_start:
{
lean_object* v___x_147_; 
v___x_147_ = lean_alloc_closure((void*)(lp_kvstore_KvStore_deleteData), 6, 5);
lean_closure_set(v___x_147_, 0, lean_box(0));
lean_closure_set(v___x_147_, 1, lean_box(0));
lean_closure_set(v___x_147_, 2, v_inst_144_);
lean_closure_set(v___x_147_, 3, v_s_145_);
lean_closure_set(v___x_147_, 4, v_k_146_);
return v___x_147_;
}
}
LEAN_EXPORT lean_object* lp_kvstore_KvStore_Replace___redArg(lean_object* v_inst_148_, lean_object* v_pairs_149_){
_start:
{
lean_object* v___x_150_; 
v___x_150_ = lean_alloc_closure((void*)(lp_kvstore_KvStore_ofList), 5, 4);
lean_closure_set(v___x_150_, 0, lean_box(0));
lean_closure_set(v___x_150_, 1, lean_box(0));
lean_closure_set(v___x_150_, 2, v_inst_148_);
lean_closure_set(v___x_150_, 3, v_pairs_149_);
return v___x_150_;
}
}
LEAN_EXPORT lean_object* lp_kvstore_KvStore_Replace(lean_object* v_00_u03b1_151_, lean_object* v_00_u03b2_152_, lean_object* v_inst_153_, lean_object* v___s_154_, lean_object* v_pairs_155_){
_start:
{
lean_object* v___x_156_; 
v___x_156_ = lean_alloc_closure((void*)(lp_kvstore_KvStore_ofList), 5, 4);
lean_closure_set(v___x_156_, 0, lean_box(0));
lean_closure_set(v___x_156_, 1, lean_box(0));
lean_closure_set(v___x_156_, 2, v_inst_153_);
lean_closure_set(v___x_156_, 3, v_pairs_155_);
return v___x_156_;
}
}
LEAN_EXPORT lean_object* lp_kvstore_KvStore_Replace___boxed(lean_object* v_00_u03b1_157_, lean_object* v_00_u03b2_158_, lean_object* v_inst_159_, lean_object* v___s_160_, lean_object* v_pairs_161_){
_start:
{
lean_object* v_res_162_; 
v_res_162_ = lp_kvstore_KvStore_Replace(v_00_u03b1_157_, v_00_u03b2_158_, v_inst_159_, v___s_160_, v_pairs_161_);
lean_dec_ref(v___s_160_);
return v_res_162_;
}
}
LEAN_EXPORT uint8_t lp_kvstore_KvStore_Len___redArg___lam__0(lean_object* v_s_163_, lean_object* v_k_164_){
_start:
{
lean_object* v___x_165_; 
v___x_165_ = lean_apply_1(v_s_163_, v_k_164_);
if (lean_obj_tag(v___x_165_) == 0)
{
uint8_t v___x_166_; 
v___x_166_ = 0;
return v___x_166_;
}
else
{
uint8_t v___x_167_; 
lean_dec_ref_known(v___x_165_, 1);
v___x_167_ = 1;
return v___x_167_;
}
}
}
LEAN_EXPORT lean_object* lp_kvstore_KvStore_Len___redArg___lam__0___boxed(lean_object* v_s_168_, lean_object* v_k_169_){
_start:
{
uint8_t v_res_170_; lean_object* v_r_171_; 
v_res_170_ = lp_kvstore_KvStore_Len___redArg___lam__0(v_s_168_, v_k_169_);
v_r_171_ = lean_box(v_res_170_);
return v_r_171_;
}
}
LEAN_EXPORT lean_object* lp_kvstore_KvStore_Len___redArg(lean_object* v_s_172_, lean_object* v_keys_173_){
_start:
{
lean_object* v___f_174_; lean_object* v___x_175_; lean_object* v___x_176_; 
v___f_174_ = lean_alloc_closure((void*)(lp_kvstore_KvStore_Len___redArg___lam__0___boxed), 2, 1);
lean_closure_set(v___f_174_, 0, v_s_172_);
v___x_175_ = lean_unsigned_to_nat(0u);
v___x_176_ = l_List_countP_go___redArg(v___f_174_, v_keys_173_, v___x_175_);
return v___x_176_;
}
}
LEAN_EXPORT lean_object* lp_kvstore_KvStore_Len(lean_object* v_00_u03b1_177_, lean_object* v_00_u03b2_178_, lean_object* v_inst_179_, lean_object* v_s_180_, lean_object* v_keys_181_){
_start:
{
lean_object* v___x_182_; 
v___x_182_ = lp_kvstore_KvStore_Len___redArg(v_s_180_, v_keys_181_);
return v___x_182_;
}
}
LEAN_EXPORT lean_object* lp_kvstore_KvStore_Len___boxed(lean_object* v_00_u03b1_183_, lean_object* v_00_u03b2_184_, lean_object* v_inst_185_, lean_object* v_s_186_, lean_object* v_keys_187_){
_start:
{
lean_object* v_res_188_; 
v_res_188_ = lp_kvstore_KvStore_Len(v_00_u03b1_183_, v_00_u03b2_184_, v_inst_185_, v_s_186_, v_keys_187_);
lean_dec_ref(v_inst_185_);
return v_res_188_;
}
}
lean_object* initialize_Init(uint8_t builtin);
lean_object* initialize_Init(uint8_t builtin);
void lean_initialize_runtime_module();
static bool _G_initialized = false;
LEAN_EXPORT lean_object* initialize_kvstore_KvStore_Map(uint8_t builtin) {
lean_object * res;
if (_G_initialized) return lean_io_result_mk_ok(lean_box(0));
_G_initialized = true;
lean_initialize_runtime_module();
res = initialize_Init(builtin);
if (lean_io_result_is_error(res)) return res;
lean_dec_ref(res);
res = initialize_Init(builtin);
if (lean_io_result_is_error(res)) return res;
lean_dec_ref(res);
return lean_io_result_mk_ok(lean_box(0));
}
#ifdef __cplusplus
}
#endif
