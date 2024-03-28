use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::ArrangeByKey;
use differential_dataflow::operators::Join;
use differential_dataflow::{AsCollection, Collection};
use dogsdogsdogs::operators::half_join;
use serde::{Deserialize, Serialize};
use timely::dataflow::operators::Map;
use timely::dataflow::Scope;
use timely::progress::Antichain;

/// 用户 ID
#[derive(Serialize, Deserialize, Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash, Copy)]
pub struct Uid(u64);
/// 订单 ID
#[derive(Serialize, Deserialize, Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash, Copy)]
pub struct Oid(u64);
/// 省份 ID
#[derive(Serialize, Deserialize, Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash, Copy)]
pub struct Pid(u64);

/// 订单
#[derive(Serialize, Deserialize, Clone, Debug, Ord, PartialOrd, Eq, PartialEq)]
pub struct Order {
    pub oid: Oid,
    pub price: u64,
    pub uid: Uid,
}

/// 用户
#[derive(Serialize, Deserialize, Clone, Debug, Ord, PartialOrd, Eq, PartialEq)]
pub struct User {
    pub uid: Uid,
    pub pid: Pid,
}

/// 省份
#[derive(Serialize, Deserialize, Clone, Debug, Ord, PartialOrd, Eq, PartialEq)]
pub struct Province {
    pub pid: Pid,
    pub name: String,
}

// 普通 join
pub fn regular_join<S>(
    order: &Collection<S, Order>,
    user: &Collection<S, User>,
    province: &Collection<S, Province>,
) -> Collection<S, (Order, User, Province)>
where
    S: Scope,
    S::Timestamp: Lattice,
{
    order
        .map(|o| (o.uid, o))
        .join_map(&user.map(|u| (u.uid, u)), |_, o, u| {
            (u.pid, (o.clone(), u.clone()))
        })
        .join_map(&province.map(|p| (p.pid, p)), |_, (o, u), p| {
            (o.clone(), u.clone(), p.clone())
        })
}

// 普通 join, 这里是直接使用 arrangement 本身的 join, 可以直观的看出创建了哪些 arrangement
pub fn regular_join_core<S>(
    order: &Collection<S, Order>,
    user: &Collection<S, User>,
    province: &Collection<S, Province>,
) -> Collection<S, (Order, User, Province)>
where
    S: Scope,
    S::Timestamp: Lattice,
{
    let order = order.map(|o| (o.uid, o)).arrange_by_key();
    let user = user.map(|u| (u.uid, u)).arrange_by_key();
    let province = province.map(|p| (p.pid, p)).arrange_by_key();

    // 这里额外产生了一个 arrangement
    let intermediate = order
        .join_core(&user, |_, o, u| Some((u.pid, (o.clone(), u.clone()))))
        .arrange_by_key();

    intermediate.join_core(&province, |_, (o, u), p| {
        Some((o.clone(), u.clone(), p.clone()))
    })
}

// 使用 delta join 技术来消除临时的 arrangement。 前提是需要创建以各个 input 关联字段为 Key 的 arrangement, 一般是 primary key, foreign key
// 参考:
// - https://materialize.com/blog/maintaining-joins-using-few-resources/
// - https://materialize.com/blog/delta-joins/
// - https://github.com/TimelyDataflow/differential-dataflow/blob/e153706/dogsdogsdogs/examples/delta_query2.rs
pub fn delta_join<S>(
    order: &Collection<S, Order>,
    user: &Collection<S, User>,
    province: &Collection<S, Province>,
) -> Collection<S, (Order, User, Province)>
where
    // 这里指定时间类型为 u64, 主要为为了方便实现 `frontier_func`，事实上任意 S::Timestamp: Lattice + Clone
    // 外加 [`step_back`](https://github.com/MaterializeInc/materialize/blob/4567acf28cfc56f515db87c49bc8d78cd00897e2/src/compute/src/render/mod.rs#L1098-L1100) 都可以满足要求，
    S: Scope<Timestamp = u64>,
{
    let order_arrange = order.map(|o| (o.uid, o)).arrange_by_key();
    // 这里 user 被 arrange 了两次，分别以 uid, pid 为 key
    let user_uid_arrange = user.map(|u| (u.uid, u)).arrange_by_key();
    let user_pid_arrange = user.map(|u| (u.pid, u)).arrange_by_key();
    let province_arrange = province.map(|p| (p.pid, p)).arrange_by_key();

    let order_change = order
        .inner
        .map(|(o, t, r)| ((o.uid, o, t.clone()), t, r))
        .as_collection();
    let user_change = user
        .inner
        .map(|(u, t, r)| ((u.uid, u, t.clone()), t, r))
        .as_collection();
    let province_change = province
        .inner
        .map(|(p, t, r)| ((p.pid, p, t.clone()), t, r))
        .as_collection();

    let frontier_func = |time: &u64, antichain: &mut Antichain<u64>| {
        antichain.insert(time.saturating_sub(1));
    };

    // delta join 逻辑上需要定义 join 的对象的优先级, 优先级高的可以看到其他对象同一时刻的更新
    // 这里我们定义优先级为 order < user < province

    // 订单更新产生的数据
    let order_update = half_join(
        &order_change,
        user_uid_arrange,
        frontier_func,
        |t1, t2| t1 < t2, // P(order) < P(user) 不能看到同一时刻的更新
        |_, o, u| (u.pid, (o.clone(), u.clone())),
    )
    .map(|((k, v), t)| (k, v, t));
    let order_update = half_join(
        &order_update,
        province_arrange.clone(),
        frontier_func,
        |t1, t2| t1 < t2, // P(order) < P(province) 不能看到同一时刻的更新
        |_, (o, u), p| (o.clone(), u.clone(), p.clone()),
    );

    // 用户更新产生的数据
    let user_update = half_join(
        &user_change,
        order_arrange.clone(),
        frontier_func,
        |t1, t2| t1 <= t2, // P(user) > P(order) 可以看到同一时刻的更新
        |_, u, o| (u.pid, (o.clone(), u.clone())),
    )
    .map(|((k, v), t)| (k, v, t));
    let user_update = half_join(
        &user_update,
        province_arrange,
        frontier_func,
        |t1, t2| t1 < t2, // P(user) < P(province) 不能看到同一时刻的更新
        |_, (o, u), p| (o.clone(), u.clone(), p.clone()),
    );

    // 省份更新产生的数据
    let province_update = half_join(
        &province_change,
        user_pid_arrange,
        frontier_func,
        |t1, t2| t1 <= t2, // P(province) > P(user) 可以看到同一时刻的更新
        |_, p, u| (u.uid, (u.clone(), p.clone())),
    )
    .map(|((k, v), t)| (k, v, t));
    let province_update = half_join(
        &province_update,
        order_arrange,
        frontier_func,
        |t1, t2| t1 <= t2, // P(province) > P(order) 可以看到同一时刻的更新
        |_, (u, p), o| (o.clone(), u.clone(), p.clone()),
    );

    // 汇聚所有更新的数据
    order_update
        .concat(&user_update)
        .concat(&province_update)
        .inner
        .map(|((d, t), _, r)| (d, t, r))
        .as_collection()
}

// 使用 secondary key 的 delta join.
// 在 `delta_join` 中，User 表创建了两个 arrangement,分别以 uid,pid 为 key。这样就有可能出现一个问题，如果 User 表
// 有很多 column, 这回导致这些 column 占用的空间都被 double 了。这里使用 [Late Materialization](https://github.com/frankmcsherry/blog/blob/master/posts/2020-11-18.md#joins-in-materialize-late-materialization)
// 来减少内存占用， 主要原理是通过创建 secondary index 来避免拷贝整个对象，以最开始提到的问题为例子：
// 两个 arrangement 的元素分别是 (uid, user), (pid, user), 使用 secondary index 后会改变成 (uid, user), (pid, uid)，
// 可以看到第二个 arrangement 中使用 uid 替换了 user，这样就避免了拷贝 user 中的其他 column
// 缺点是 secondary index 需要多一次 half_join 来关联到完整的数据。换句话说：通过增加计算开销来较少内存占用。
pub fn delta_join_late_materialization<S>(
    order: &Collection<S, Order>,
    user: &Collection<S, User>,
    province: &Collection<S, Province>,
) -> Collection<S, (Order, User, Province)>
where
    S: Scope<Timestamp = u64>,
{
    let order_arrange = order.map(|o| (o.uid, o)).arrange_by_key();
    let user_uid_arrange = user.map(|u| (u.uid, u)).arrange_by_key();
    // 与 `delta_join` 不同， 这里的 value 从 User 变成了 Uid, 避免了拷贝整个 User
    let user_pid_arrange = user.map(|u| (u.pid, u.uid)).arrange_by_key();
    let province_arrange = province.map(|p| (p.pid, p)).arrange_by_key();

    let order_change = order
        .inner
        .map(|(o, t, r)| ((o.uid, o, t.clone()), t, r))
        .as_collection();
    let user_change = user
        .inner
        .map(|(u, t, r)| ((u.uid, u, t.clone()), t, r))
        .as_collection();
    let province_change = province
        .inner
        .map(|(p, t, r)| ((p.pid, p, t.clone()), t, r))
        .as_collection();

    let frontier_func = |time: &u64, antichain: &mut Antichain<u64>| {
        antichain.insert(time.saturating_sub(1));
    };

    // delta join 逻辑上需要定义 join 的对象的优先级, 优先级高的可以看到其他对象同一时刻的更新
    // 这里我们定义优先级为 order < user < province

    // 订单更新产生的数据
    let order_update = half_join(
        &order_change,
        user_uid_arrange.clone(),
        frontier_func,
        |t1, t2| t1 < t2, // P(order) < P(user) 不能看到同一时刻的更新
        |_, o, u| (u.pid, (o.clone(), u.clone())),
    )
    .map(|((k, v), t)| (k, v, t));
    let order_update = half_join(
        &order_update,
        province_arrange.clone(),
        frontier_func,
        |t1, t2| t1 < t2, // P(order) < P(province) 不能看到同一时刻的更新
        |_, (o, u), p| (o.clone(), u.clone(), p.clone()),
    );

    // 用户更新产生的数据
    let user_update = half_join(
        &user_change,
        order_arrange.clone(),
        frontier_func,
        |t1, t2| t1 <= t2, // P(user) > P(order) 可以看到同一时刻的更新
        |_, u, o| (u.pid, (o.clone(), u.clone())),
    )
    .map(|((k, v), t)| (k, v, t));
    let user_update = half_join(
        &user_update,
        province_arrange,
        frontier_func,
        |t1, t2| t1 < t2, // P(user) < P(province) 不能看到同一时刻的更新
        |_, (o, u), p| (o.clone(), u.clone(), p.clone()),
    );

    // 省份更新产生的数据
    let province_update = half_join(
        &province_change,
        user_pid_arrange,
        frontier_func,
        |t1, t2| t1 <= t2, // P(province) > P(user) 可以看到同一时刻的更新
        |_, p, uid| (*uid, p.clone()),
    )
    .map(|((k, v), t)| (k, v, t));
    // 这是相比 `delta_join` 多的一步，这里需要通过 secondary key 重新关联到 user
    let province_update = half_join(
        &province_update,
        user_uid_arrange,
        frontier_func,
        |t1, t2| t1 <= t2, // P(province) > P(user) 可以看到同一时刻的更新
        |_, p, u| (u.uid, (u.clone(), p.clone())),
    )
    .map(|((k, v), t)| (k, v, t));
    let province_update = half_join(
        &province_update,
        order_arrange,
        frontier_func,
        |t1, t2| t1 <= t2, // P(province) > P(order) 可以看到同一时刻的更新
        |_, (u, p), o| (o.clone(), u.clone(), p.clone()),
    );

    // 汇聚所有更新的数据
    order_update
        .concat(&user_update)
        .concat(&province_update)
        .inner
        .map(|((d, t), _, r)| (d, t, r))
        .as_collection()
}
